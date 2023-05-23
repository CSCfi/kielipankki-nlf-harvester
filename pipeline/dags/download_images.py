"""
Depth-first parallelized download procedure that creates a DAG for each collection.
Collections are split into chunks and downloaded into disk images.
"""

from datetime import timedelta
from pathlib import Path
import os

from airflow.operators.empty import EmptyOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.hooks.base import BaseHook
from airflow.decorators import task, task_group, dag
from airflow.models import DagRun

from harvester.pmh_interface import PMH_API
from harvester import utils

from importlib import reload
reload(utils)

from operators.custom_operators import (
    SaveMetsSFTPOperator,
    SaveAltosSFTPOperator,
)

BASE_PATH = "/scratch/project_2006633/nlf-harvester/images"
TMPDIR = "/local_scratch/robot_2006633_puhti/harvester-temp"
SSH_CONN_ID = "puhti_conn"
HTTP_CONN_ID = "nlf_http_conn"
SET_IDS = ["col-361"]
BINDING_BASE_PATH = Path("/home/ubuntu/binding_ids_all")

default_args = {
    "owner": "Kielipankki",
    "start_date": "2023-22-05",
    "retry_delay": timedelta(minutes=5),
    "retries": 3,
}


http_conn = BaseHook.get_connection(HTTP_CONN_ID)
api = PMH_API(url=http_conn.host)


for set_id in SET_IDS:

    current_dag_id = f"image_download_{set_id}"

    @dag(
        dag_id=current_dag_id,
        schedule="@once",
        catchup=False,
        default_args=default_args,
        doc_md=__doc__,
    )
    def download_dag():

        @task
        def get_most_recent_dag_run(dag_id):
            dag_runs = DagRun.find(dag_id=dag_id, state="success")
            dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
            last_run = dag_runs[0].execution_date.strftime('%Y-%m-%d') if dag_runs else None
            return last_run

        last_run = get_most_recent_dag_run(current_dag_id)

        begin_download = EmptyOperator(task_id="begin_download")

        cancel_pipeline = EmptyOperator(task_id="cancel_pipeline")

        @task.branch(task_id="check_api_availability")
        def check_api_availability():
            """
            Check if API is responding and download can begin. If not, cancel pipeline.
            """
            api_ok = HttpSensor(
                task_id="http_sensor", http_conn_id=HTTP_CONN_ID, endpoint="/"
            ).poke(context={})

            if api_ok:
                return "begin_download"
            else:
                return "cancel_pipeline"

        check_api_availability = check_api_availability()

        check_api_availability >> [begin_download, cancel_pipeline]

        @task_group(group_id="download_set")
        def download_set(set_id, api, ssh_conn_id):

            with open(BINDING_BASE_PATH / set_id / "binding_ids", "r") as f:
                bindings = f.read().splitlines()
            
            for image in utils.assign_bindings_to_images(bindings, 200):
                if image["bindings"]:

                    @task(task_id=f"ensure_image_{set_id}_{image['prefix']}")
                    def ensure_image(image):
                        """
                        Create an empty directory for image contents or extract an existing disk image.
                        """

                        image_base_name = f"image_{set_id}_{image['prefix']}".rstrip("_")
                        image_dir_path = os.path.join(BASE_PATH, image_base_name)
                        
                        # Check if image exists
                        ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)
                        with ssh_hook.get_conn() as ssh_client:
                            sftp_client = ssh_client.open_sftp()
                            utils.make_intermediate_dirs(
                                sftp_client=sftp_client,
                                remote_directory=BASE_PATH,
                            )
                            # if exists, extract with a bash command
                            if f"{image_base_name}.sqfs" in sftp_client.listdir(BASE_PATH):
                                sftp_client.chdir(BASE_PATH)
                                ssh_client.exec_command(f"unsquashfs -d {image_dir_path} {image_dir_path}.sqfs")
                                ssh_client.exec_command(f"rm {image_dir_path}.sqfs")
                            
                            # if not exist, create image folder
                            else:
                                utils.make_intermediate_dirs(
                                    sftp_client=sftp_client,
                                    remote_directory=image_dir_path,
                                )


                    @task_group(group_id=f"download_image_{set_id}_{image['prefix']}")
                    def download_image(image):

                        @task(task_id="download_binding_batch", trigger_rule="none_skipped")
                        def download_binding_batch(batch):
                            ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)
                            with ssh_hook.get_conn() as ssh_client:
                                sftp_client = ssh_client.open_sftp()

                                for dc_identifier in batch:
                                    binding_id = utils.binding_id_from_dc(dc_identifier)
                                    image_base_name = f"image_{set_id}_{image['prefix']}".rstrip("_")
                                    binding_path = os.path.join(BASE_PATH, image_base_name, utils.binding_download_location(binding_id))
                                    tmp_binding_path = os.path.join(TMPDIR, utils.binding_download_location(binding_id))

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
                            image_base_name = f"image_{set_id}_{image['prefix']}".rstrip("_")
                            image_dir_path = os.path.join(BASE_PATH, image_base_name)

                            ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)
                            with ssh_hook.get_conn() as ssh_client:
                                _, stdout, _ = ssh_client.exec_command(f"mksquashfs {image_dir_path} {image_dir_path}.sqfs")
                                if stdout.channel.recv_exit_status() != 0:
                                    raise Exception(f"Creation of image {image_dir_path}.sqfs failed")
                                ssh_client.exec_command(f"rm -r {image_dir_path}")
                            

                        batch_downloads = []
                        for batch in utils.split_into_download_batches(image["bindings"]):
                            batch_downloads.append(download_binding_batch(batch=batch))
                        
                        batch_downloads >> create_image(image)
                    
                    ensure_image(image) >> download_image(image)
            

        @task(task_id="clear_temp_directory", trigger_rule="all_done")
        def clear_temp_dir():
            ssh_hook = SSHHook(ssh_conn_id=SSH_CONN_ID)
            with ssh_hook.get_conn() as ssh_client:
                ssh_client.exec_command(f"rm -r {TMPDIR}/*")

        (
            begin_download
            >> last_run
            >> download_set(set_id=set_id, api=api, ssh_conn_id=SSH_CONN_ID)
            >> clear_temp_dir()
        )

    download_dag()
