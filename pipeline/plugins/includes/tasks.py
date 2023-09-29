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
    CreateDistributionOperator,
    StowBindingBatchOperator,
)


@task.branch(task_id="check_if_download_should_begin")
def check_if_download_should_begin(set_id, binding_list_dir, http_conn_id):
    """
    Check if API is responding and if there are new bindings to download.
    If not, cancel pipeline.
    """

    def newest_dag_run():
        dag_runs = DagRun.find(dag_id=f"image_download_{set_id}", state="running")
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

    bindings = utils.read_bindings(binding_list_dir, set_id)

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
    image_size,
    api,
    ssh_conn_id,
    initial_download,
    pathdict,
):

    bindings = utils.read_bindings(pathdict["BINDING_LIST_DIR"], set_id)

    if initial_download:
        image_split = utils.assign_bindings_to_images(bindings, image_size)
        utils.save_image_split(image_split, pathdict["IMAGE_SPLIT_DIR"], set_id)

    else:
        image_split = utils.assign_update_bindings_to_images(
            bindings, pathdict["IMAGE_SPLIT_DIR"] / f"{set_id}_images.json"
        )

    image_downloads = []

    for prefix in image_split:
        if image_split[prefix]:

            @task_group(group_id=f"download_image_{set_id}_{prefix}".rstrip("_"))
            def download_image(image):

                if prefix:
                    image_base_name = f"{set_id}_{prefix}"
                else:
                    image_base_name = set_id

                file_download_dir = pathdict["TMPDIR_ROOT"] / image_base_name
                image_path = (
                    pathdict["OUTPUT_DIR"] / "images" / (image_base_name + ".sqfs")
                )
                target_directory = pathdict["OUTPUT_DIR"] / "targets"
                tar_directory = pathdict["OUTPUT_DIR"] / "tar" / image_base_name

                prepare_download_location = PrepareDownloadLocationOperator(
                    task_id=f"prepare_download_location_{image_base_name}",
                    trigger_rule="none_skipped",
                    ssh_conn_id=ssh_conn_id,
                    ensure_dirs=[file_download_dir, tar_directory, target_directory],
                    old_image_path=image_path,
                    extra_bin_dir=pathdict["EXTRA_BIN_DIR"],
                )

                create_image = CreateDistributionOperator(
                    task_id=f"create_image_{image_base_name}",
                    trigger_rule="none_skipped",
                    ssh_conn_id=ssh_conn_id,
                    data_source=tar_directory,
                    target_path=image_path,
                    extra_bin_dir=pathdict["EXTRA_BIN_DIR"],
                )

                (
                    prepare_download_location
                    >> StowBindingBatchOperator.partial(
                        task_id="download_binding_batch",
                        trigger_rule="none_skipped",
                        ssh_conn_id=ssh_conn_id,
                        tmp_download_directory=pathdict["TMPDIR_ROOT"]
                        / image_base_name,
                        tar_directory=tar_directory,
                        api=api,
                    ).expand(
                        batch_with_index=utils.split_into_download_batches(
                            image_split[image]
                        )
                    )
                    >> create_image
                )

            image_download_tg = download_image(prefix)
            if image_downloads:
                image_downloads[-1] >> image_download_tg
            image_downloads.append(image_download_tg)


@task(task_id="clear_temporary_directory", trigger_rule="all_done")
def clear_temporary_directory(ssh_conn_id, tmpdir_root):
    ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)
    with ssh_hook.get_conn() as ssh_client:
        ssh_client.exec_command(f"rm -r {tmpdir_root}/*")


@task(task_id="create_restic_snapshot", trigger_rule="all_done")
def create_restic_snapshot(ssh_conn_id, script_path, output_dir):
    ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)
    with ssh_hook.get_conn() as ssh_client:
        with open("/home/ubuntu/restic_env.yaml", "r") as fobj:
            envs = yaml.load(fobj, Loader=yaml.FullLoader)

        envs_str = " ".join([f"export {key}={value};" for key, value in envs.items()])
        print("Creating snapshot of downloaded images")
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
