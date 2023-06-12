from airflow.decorators import task, task_group
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.ssh.hooks.ssh import SSHHook

from urllib.error import HTTPError

from harvester import utils
from operators.custom_operators import (
    PrepareDownloadLocationOperator,
    CreateImageOperator,
    DownloadBindingBatchOperator,
)


@task.branch(task_id="check_if_download_should_begin")
def check_if_download_should_begin(set_id, binding_base_path, http_conn_id):
    """
    Check if API is responding and if there are new bindings to download.
    If not, cancel pipeline.
    """
    api_ok = HttpSensor(
        task_id="http_sensor", http_conn_id=http_conn_id, endpoint="/"
    ).poke(context={})

    bindings = utils.read_bindings(binding_base_path, set_id)

    if not bindings:
        print("No new bindings after previous download.")
        return "cancel_pipeline"
    if not api_ok:
        raise HTTPError("NLF API is not responding.")
    else:
        return "begin_download"


@task_group(group_id="download_set")
def download_set(
    set_id,
    image_size,
    api,
    ssh_conn_id,
    initial_download,
    image_split_dir,
    binding_base_path,
    base_path,
    tmpdir,
):

    bindings = utils.read_bindings(binding_base_path, set_id)

    if initial_download:
        image_split = utils.assign_bindings_to_images(bindings, image_size)
        utils.save_image_split(image_split, image_split_dir, set_id)

    else:
        image_split = utils.assign_update_bindings_to_images(
            bindings, image_split_dir / f"{set_id}_images.json"
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

                prepare_download_location = PrepareDownloadLocationOperator(
                    task_id=f"prepare_download_location_{image_base_name}",
                    ssh_conn_id=ssh_conn_id,
                    base_path=base_path,
                    image_base_name=image_base_name,
                )

                create_image = CreateImageOperator(
                    task_id=f"create_image_{image_base_name}",
                    ssh_conn_id=ssh_conn_id,
                    base_path=base_path,
                    image_base_name=image_base_name,
                )

                (
                    prepare_download_location
                    >> DownloadBindingBatchOperator.partial(
                        task_id=f"download_binding_batch",
                        trigger_rule="none_failed_min_one_success",
                        ssh_conn_id=ssh_conn_id,
                        base_path=base_path,
                        image_base_name=image_base_name,
                        tmpdir=tmpdir,
                        api=api,
                    ).expand(
                        batch=utils.split_into_download_batches(image_split[image])
                    )
                    >> create_image
                )

            image_download_tg = download_image(prefix)
            if image_downloads:
                image_downloads[-1] >> image_download_tg
            image_downloads.append(image_download_tg)


@task(task_id="clear_temp_directory", trigger_rule="all_done")
def clear_temp_dir(ssh_conn_id, tmpdir):
    ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)
    with ssh_hook.get_conn() as ssh_client:
        ssh_client.exec_command(f"rm -r {tmpdir}/*")
