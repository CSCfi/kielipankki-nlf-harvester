from airflow.decorators import task, task_group
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.ssh.hooks.ssh import SSHHook

from urllib.error import HTTPError
import os

from harvester import utils
from operators.custom_operators import (
    SaveMetsSFTPOperator,
    SaveAltosSFTPOperator,
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
        image_split = utils.assign_bindings_to_images(bindings, 150)
        utils.save_image_split(image_split, image_split_dir, set_id)

    else:
        image_split = utils.assign_update_bindings_to_images(
            bindings, image_split_dir / f"{set_id}_images.json"
        )

    image_downloads = []

    for image in image_split:
        if image["bindings"]:

            @task_group(group_id=f"download_image_{set_id}_{image['prefix']}")
            def download_image(image):
                @task(task_id=f"prepare_download_location_{set_id}_{image['prefix']}")
                def prepare_download_location(image):
                    """
                    Create an empty directory for image contents or extract an existing disk image.
                    """

                    image_base_name = f"{set_id}_{image['prefix']}".rstrip("_")
                    image_dir_path = os.path.join(base_path, image_base_name)

                    # Check if image exists
                    ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)
                    with ssh_hook.get_conn() as ssh_client:
                        sftp_client = ssh_client.open_sftp()
                        utils.make_intermediate_dirs(
                            sftp_client=sftp_client,
                            remote_directory=base_path,
                        )
                        # if exists, extract with a bash command
                        if f"{image_base_name}.sqfs" in sftp_client.listdir(base_path):
                            sftp_client.chdir(base_path)
                            ssh_client.exec_command(
                                f"unsquashfs -d {image_dir_path} {image_dir_path}.sqfs"
                            )

                        # if not exist, create image folder
                        else:
                            utils.make_intermediate_dirs(
                                sftp_client=sftp_client,
                                remote_directory=image_dir_path,
                            )

                @task(
                    task_id="download_binding_batch",
                    trigger_rule="none_skipped",
                )
                def download_binding_batch(batch):
                    ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)
                    with ssh_hook.get_conn() as ssh_client:
                        sftp_client = ssh_client.open_sftp()

                        for dc_identifier in batch:
                            binding_id = utils.binding_id_from_dc(dc_identifier)
                            image_base_name = f"{set_id}_{image['prefix']}".rstrip("_")
                            binding_path = os.path.join(
                                base_path,
                                image_base_name,
                                utils.binding_download_location(binding_id),
                            )
                            tmp_binding_path = os.path.join(
                                tmpdir,
                                utils.binding_download_location(binding_id),
                            )

                            SaveMetsSFTPOperator(
                                task_id=f"save_mets_{binding_id}",
                                api=api,
                                sftp_client=sftp_client,
                                ssh_client=ssh_client,
                                tmpdir=tmp_binding_path,
                                dc_identifier=dc_identifier,
                                binding_path=binding_path,
                                file_dir="mets",
                            ).execute(context={})

                            SaveAltosSFTPOperator(
                                task_id=f"save_altos_{binding_id}",
                                mets_path=f"{binding_path}/mets",
                                sftp_client=sftp_client,
                                ssh_client=ssh_client,
                                tmpdir=tmp_binding_path,
                                dc_identifier=dc_identifier,
                                binding_path=binding_path,
                                file_dir="alto",
                            ).execute(context={})

                            ssh_client.exec_command(f"rm -r {tmp_binding_path}")

                @task(task_id=f"create_image_{set_id}_{image['prefix']}")
                def create_image(image):
                    print(f"Creating image_{set_id}_{image['prefix']} on Puhti")
                    image_base_name = f"{set_id}_{image['prefix']}".rstrip("_")
                    image_dir_path = os.path.join(base_path, image_base_name)

                    ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)
                    with ssh_hook.get_conn() as ssh_client:
                        ssh_client.exec_command(f"rm {image_dir_path}.sqfs")
                        _, stdout, stderr = ssh_client.exec_command(
                            f"mksquashfs {image_dir_path} {image_dir_path}.sqfs"
                        )
                        if stdout.channel.recv_exit_status() != 0:
                            raise Exception(
                                f"Creation of image {image_dir_path}.sqfs failed: {stderr.read().decode('utf-8')}"
                            )
                        ssh_client.exec_command(f"rm -r {image_dir_path}")

                batch_downloads = []
                for batch in utils.split_into_download_batches(image["bindings"]):
                    batch_downloads.append(download_binding_batch(batch=batch))

                prepare_download_location(image) >> batch_downloads >> create_image(image)

            image_download_tg = download_image(image)
            if image_downloads:
                image_downloads[-1] >> image_download_tg
            image_downloads.append(image_download_tg)


@task(task_id="clear_temp_directory", trigger_rule="all_done")
def clear_temp_dir(ssh_conn_id, tmpdir):
    ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)
    with ssh_hook.get_conn() as ssh_client:
        ssh_client.exec_command(f"rm -r {tmpdir}/*")
