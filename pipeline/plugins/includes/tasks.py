from airflow.decorators import task, task_group
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.models import DagRun

from requests.exceptions import RequestException

from datetime import date
import os

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
def check_if_download_should_begin(set_id, binding_list_dir, http_conn_id, path_config):
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

    added_bindings = utils.read_bindings(
        binding_list_dir, set_id, path_config["ADDED_BINDINGS_PREFIX"]
    )
    deleted_bindings = utils.read_bindings(
        binding_list_dir, set_id, path_config["DELETED_BINDINGS_PREFIX"]
    )

    if not added_bindings and not deleted_bindings:
        print("No new or deleted bindings after previous download.")
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
        path_config["BINDING_LIST_DIR"], set_id, path_config["ADDED_BINDINGS_PREFIX"]
    )
    deleted_bindings = utils.read_bindings(
        path_config["BINDING_LIST_DIR"], set_id, path_config["DELETED_BINDINGS_PREFIX"]
    )

    prefixes = [str(i) for i in range(10, 20)] + [str(i) for i in range(2, 10)]

    if initial_download:
        subset_split = utils.assign_bindings_to_subsets(
            added_binding_dc_identifiers=added_bindings,
            deleted_binding_dc_identifiers=[],
            prefixes=prefixes,
        )
        utils.save_subset_split(subset_split, path_config["SUBSET_SPLIT_DIR"], set_id)

    else:
        subset_split = utils.assign_update_bindings_to_subsets(
            added_bindings_dc_identifiers=added_bindings,
            deleted_bindings_dc_identifiers=deleted_bindings,
            subset_split_file=path_config["SUBSET_SPLIT_DIR"]
            / f"{set_id}_subsets.json",
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
                target_path = path_config["ZIP_CREATION_DIR"] / (
                    subset_base_name + ".zip"
                )
                intermediate_zip_directory = (
                    path_config["INTERMEDIATE_ZIP_DIR"] / subset_base_name
                )
                published_zip_path = path_config["PUBLISHED_DATA_DIR"] / (
                    subset_base_name + ".zip"
                )

                prepare_download_location = PrepareDownloadLocationOperator(
                    task_id=f"prepare_download_location_{subset_base_name}",
                    trigger_rule="none_skipped",
                    ssh_conn_id=ssh_conn_id,
                    ensure_dirs=[
                        file_download_dir,
                        intermediate_zip_directory,
                        path_config["ZIP_CREATION_DIR"],
                    ],
                    old_target_path=(None if initial_download else published_zip_path),
                    new_target_path=(None if initial_download else target_path),
                )

                remove_deleted_bindings = RemoveDeletedBindingsOperator(
                    task_id=f"remove_deleted_bindings_{subset_base_name}",
                    ssh_conn_id=ssh_conn_id,
                    zip_path=target_path,
                    deleted_bindings_list=subset_split[subset]["deleted"],
                )

                if subset_split[subset]["added"]:
                    create_target = CreateTargetOperator(
                        task_id=f"create_target_{subset_base_name}",
                        trigger_rule="none_skipped",
                        ssh_conn_id=ssh_conn_id,
                        data_source=intermediate_zip_directory,
                        target_path=target_path,
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
                            retries=2,
                        ).expand(
                            batch_with_index=utils.split_into_download_batches(
                                subset_split[subset]["added"]
                            )
                        )
                        >> create_target
                        >> remove_deleted_bindings
                    )
                else:
                    (prepare_download_location >> remove_deleted_bindings)

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


@task(task_id="generate_listings")
def generate_listings(ssh_conn_id, set_id, path_config):
    """
    The following listings in the listings directory are generated:
      * bindings that were successfully added
      * bindings that were supposed to be added, but failed
      * bindings that were deleted
      * bindings that were supposed to be deleted, but are still present
    If a listing is empty, it is not generated.

    We also clear the version string of the not-yet-backed up published data.
    """
    added_binding_ids = set(
        [
            url.split("/")[-1]
            for url in utils.read_bindings(
                path_config["BINDING_LIST_DIR"],
                set_id,
                path_config["ADDED_BINDINGS_PREFIX"],
            )
        ]
    )
    deleted_binding_ids = set(
        [
            url.split("/")[-1]
            for url in utils.read_bindings(
                path_config["BINDING_LIST_DIR"],
                set_id,
                path_config["DELETED_BINDINGS_PREFIX"],
            )
        ]
    )

    ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)
    with ssh_hook.get_conn() as ssh_client:
        command = f'find {path_config["PUBLISHED_DATA_DIR"]} -type f -name "*.zip" -exec unzip -l {{}} \; | grep -Po "[0-9]+(?=_METS)"'
        _, stdout, stderr = ssh_client.exec_command(command)
        bindings_found_lines = [line.rstrip("\n") for line in stdout.readlines()]
    binding_ids_with_mets_files = set(bindings_found_lines)
    binding_ids_added_successfully = added_binding_ids.intersection(
        binding_ids_with_mets_files
    )
    binding_ids_failed_to_add = added_binding_ids.difference(
        binding_ids_with_mets_files
    )
    binding_ids_deleted_successfully = deleted_binding_ids.difference(
        binding_ids_with_mets_files
    )
    binding_ids_failed_to_delete = deleted_binding_ids.intersection(
        binding_ids_with_mets_files
    )

    listings = [
        ("added_bindings.txt", binding_ids_added_successfully),
        ("added_bindings_FAILED.txt", binding_ids_failed_to_add),
        ("deleted_bindings.txt", binding_ids_deleted_successfully),
        ("deleted_bindings_FAILED.txt", binding_ids_failed_to_delete),
    ]

    if all([len(listing[1]) == 0 for listing in listings]):
        # If all listings would be empty, don't write listings
        return

    # Set up the paths for today's listings, both in the Airflow machine and Puhti
    puhti_listing_dir = (
        path_config["OUTPUT_DIR"] / "logs" / "listings" / str(date.today())
    )
    with ssh_hook.get_conn() as ssh_client:
        _, stdout, stderr = ssh_client.exec_command(
            f"umask a+rx; mkdir -p {puhti_listing_dir}"
        )
    airflow_listing_dir = path_config["AIRFLOW_LISTINGS_DIR"] / str(date.today())
    if not os.path.isdir(airflow_listing_dir):
        os.makedirs(airflow_listing_dir)

    for listing in listings:
        if len(listing[1]) == 0:
            # Skip if empty
            continue
        with open(airflow_listing_dir / listing[0], "w") as listing_file:
            for _id in listing[1]:
                # Write to Airflow machine
                listing_file.write(_id + "\n")
        with ssh_hook.get_conn() as ssh_client:
            sftp_client = ssh_client.open_sftp()
            # Then copy to Puhti
            sftp_client.put(
                str(airflow_listing_dir / listing[0]),
                str(puhti_listing_dir / listing[0]),
            )
    with ssh_hook.get_conn() as ssh_client:
        _, stdout, stderr = ssh_client.exec_command(
            f"rm -f {path_config['OUTPUT_DIR'] / 'logs' / 'latest_version_string'}"
        )


class CreateSnapshotError(Exception):
    """
    Error raised when an error occurs during the creation of a restic snapshot
    """
