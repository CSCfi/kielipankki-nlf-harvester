from airflow.decorators import task, task_group
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.models import DagRun

from requests.exceptions import RequestException

import yaml

# pylint does not understand that custom operators are not third party code
# pylint: disable=wrong-import-order

from harvester import utils
from operators.custom_operators import (
    PrepareDownloadLocationOperator,
    CreateTargetOperator,
    StowBindingBatchOperator,
    RemoveDeletedBindingsOperator,
)


@task.branch(task_id="check_if_download_should_begin")
def check_if_download_should_begin(set_id, binding_list_dir, http_conn_id):
    """
    Check if API is responding and if there are new bindings to download.
    If not, cancel pipeline.
    """

    def newest_dag_run():
        dag_runs = DagRun.find(dag_id=f"subset_download_{set_id}", state="running")
        newest = dag_runs[0]
        for dag_run in dag_runs[1:]:
            if dag_run.execution_date > newest.execution_date:
                newest = dag_run
        return newest

    try:
        api_ok = HttpSensor(
            task_id="http_sensor", http_conn_id=http_conn_id, endpoint="/"
        ).poke(context={})
    except RequestException:
        api_ok = False

    bindings = utils.read_bindings(binding_list_dir, set_id, "binding_ids")

    if not bindings:
        print("No new bindings after previous download.")
        return "cancel_pipeline"
    if not api_ok:
        dag_instance = newest_dag_run()
        task_instance = dag_instance.get_task_instance("check_if_download_should_begin")
        if task_instance.try_number < task_instance.max_tries:
            raise RequestException("NLF API is not responding.")
        else:
            # If maximum number of retries has been reached, cancel pipeline
            print("NLF API is not responding.")
            return "cancel_pipeline"
    else:
        return "begin_download"


@task_group(group_id="download_set")
def download_set(
    set_id,
    subset_size,
    api,
    ssh_conn_id,
    initial_download,
    path_config,
):
    added_bindings = utils.read_bindings(
        path_config["BINDING_LIST_DIR"], set_id, "binding_ids"
    )
    deleted_bindings = utils.read_bindings(
        path_config["BINDING_LIST_DIR"], set_id, "deleted_binding_ids"
    )

    prefixes = [str(i) for i in range(10, 20)] + [str(i) for i in range(2, 10)]

    if initial_download:
        subset_split = utils.assign_bindings_to_subsets(added_bindings, [], prefixes)
        utils.save_subset_split(subset_split, path_config["SUBSET_SPLIT_DIR"], set_id)

    else:
        subset_split = utils.assign_update_bindings_to_subsets(
            added_bindings,
            deleted_bindings,
            path_config["SUBSET_SPLIT_DIR"] / f"{set_id}_subsets.json",
        )

    subset_downloads = []

    for prefix in subset_split:
        if subset_split[prefix]:

            @task_group(group_id=f"download_subset_{set_id}_{prefix}".rstrip("_"))
            def download_subset(subset):
                if prefix:
                    subset_base_name = f"{set_id}_{prefix}"
                else:
                    subset_base_name = set_id

                file_download_dir = path_config["TMPDIR_ROOT"] / subset_base_name
                target_path = (
                    path_config["OUTPUT_DIR"] / "targets" / (subset_base_name + ".zip")
                )
                target_directory = path_config["OUTPUT_DIR"] / "targets"
                intermediate_zip_directory = (
                    path_config["OUTPUT_DIR"] / "intermediate_zip" / subset_base_name
                )
                published_zip_path = (
                    path_config["OUTPUT_DIR"] / "zip" / (subset_base_name + ".zip")
                )

                prepare_download_location = PrepareDownloadLocationOperator(
                    task_id=f"prepare_download_location_{subset_base_name}",
                    trigger_rule="none_skipped",
                    ssh_conn_id=ssh_conn_id,
                    ensure_dirs=[
                        file_download_dir,
                        intermediate_zip_directory,
                        target_directory,
                    ],
                    old_target_path=(None if initial_download else published_zip_path),
                    new_target_path=(None if initial_download else target_path),
                )

                create_target = CreateTargetOperator(
                    task_id=f"create_target_{subset_base_name}",
                    trigger_rule="none_skipped",
                    ssh_conn_id=ssh_conn_id,
                    data_source=intermediate_zip_directory,
                    target_path=target_path,
                )

                remove_deleted_bindings = RemoveDeletedBindingsOperator(
                    task_id=f"remove_deleted_bindings_{subset_base_name}",
                    ssh_conn_id=ssh_conn_id,
                    zip_path=target_path,
                    deleted_bindings_list=subset_split[subset]["deleted"],
                )

                (
                    prepare_download_location
                    >> StowBindingBatchOperator.partial(
                        task_id="download_binding_batch",
                        trigger_rule="none_skipped",
                        ssh_conn_id=ssh_conn_id,
                        tmp_download_directory=path_config["TMPDIR_ROOT"]
                        / subset_base_name,
                        intermediate_zip_directory=intermediate_zip_directory,
                        api=api,
                    ).expand(
                        batch_with_index=utils.split_into_download_batches(
                            subset_split[subset]["added"]
                        )
                    )
                    >> create_target
                    >> remove_deleted_bindings
                )

            subset_download_tg = download_subset(prefix)
            if subset_downloads:
                subset_downloads[-1] >> subset_download_tg
            subset_downloads.append(subset_download_tg)


@task(task_id="clear_temporary_directory", trigger_rule="all_done")
def clear_temporary_directory(ssh_conn_id, tmpdir_root):
    ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)
    with ssh_hook.get_conn() as ssh_client:
        ssh_client.exec_command(f"rm -r {tmpdir_root}/*")


@task(task_id="publish_to_users")
def publish_to_users(ssh_conn_id, source, destination):
    """
    Publish the data from `source` for user access in `destination`.

    To ensure that the change happens as fast as possible from the perspective of our
    end users and that they won't see halfway-written files, we do the publishing as a
    rename (`mv`) operation that is atomic. If the destination directory does not exist
    yet, it is created.

    NB: The possibility of new files appearing or old ones being removed (e.g.
    2-prefixed bindings being split into 20, 21, ... 29 or vice versa due to addition or
    removal of bindings) is not handled: if that happens, the old file(s) will remain in
    the destination directory.
    """
    ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)
    with ssh_hook.get_conn() as ssh_client:
        sftp_client = ssh_client.open_sftp()

        utils.make_intermediate_dirs(sftp_client, remote_directory=destination)

        for filename in sftp_client.listdir(str(source)):
            source_filename = source / filename
            destination_filename = destination / filename
            sftp_client.posix_rename(str(source_filename), str(destination_filename))


@task(task_id="create_restic_snapshot", trigger_rule="all_done")
def create_restic_snapshot(ssh_conn_id, script_path, output_dir):
    ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)
    with ssh_hook.get_conn() as ssh_client:
        with open("/home/ubuntu/restic_env.yaml", "r") as fobj:
            envs = yaml.load(fobj, Loader=yaml.FullLoader)

        envs_str = " ".join([f"export {key}={value};" for key, value in envs.items()])
        print("Creating snapshot of downloaded subsets")
        _, stdout, stderr = ssh_client.exec_command(
            f"{envs_str} sh {script_path} {output_dir}", get_pty=True
        )

        output = "\n".join(stdout.readlines())
        print(output)

        if stdout.channel.recv_exit_status() != 0:
            error_msg = "\n".join(stderr.readlines())
            raise CreateSnapshotError(
                f"Creating snapshot failed with error:\n{error_msg}"
            )


class CreateSnapshotError(Exception):
    """
    Error raised when an error occurs during the creation of a restic snapshot
    """
