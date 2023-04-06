"""
Depth-first parallelized download procedure that creates a DAG for each collection.
Collections are split into batches based on their size.
"""

from datetime import timedelta
from pathlib import Path

from airflow.operators.empty import EmptyOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.hooks.base import BaseHook
from airflow.decorators import task, task_group, dag

from harvester.pmh_interface import PMH_API
from harvester import utils

from operators.custom_operators import (
    SaveMetsSFTPOperator,
    SaveAltosSFTPOperator,
)

BASE_PATH = "/scratch/project_2006633/nlf-harvester/downloads"
TMPDIR = "/local_scratch/robot_2006633_puhti/harvester-temp"
SSH_CONN_ID = "puhti_conn"
HTTP_CONN_ID = "nlf_http_conn"
SET_IDS = ["col-24", "col-82", "col-361", "col-501"]
BINDING_BASE_PATH = Path("/home/ubuntu/binding_ids_all")

default_args = {
    "owner": "Kielipankki",
    "start_date": "2022-10-01",
    "retry_delay": timedelta(minutes=5),
    "retries": 3,
}


http_conn = BaseHook.get_connection(HTTP_CONN_ID)
api = PMH_API(url=http_conn.host)

for set_id in SET_IDS:

    @dag(
        dag_id=f"dynamic_batch_download_{set_id.replace(':', '_')}",
        schedule="@once",
        catchup=False,
        default_args=default_args,
        doc_md=__doc__,
    )
    def download_dag():

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
        def download_set(set_id, api, ssh_conn_id, base_path):
            @task(task_id="download_binding_batch", trigger_rule="none_skipped")
            def download_binding_batch(batch):
                ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)
                with ssh_hook.get_conn() as ssh_client:
                    sftp_client = ssh_client.open_sftp()

                    for dc_identifier in batch:
                        binding_id = utils.binding_id_from_dc(dc_identifier)

                        SaveMetsSFTPOperator(
                            task_id=f"save_mets_{binding_id}",
                            api=api,
                            sftp_client=sftp_client,
                            ssh_client=ssh_client,
                            tmpdir=TMPDIR,
                            dc_identifier=dc_identifier,
                            base_path=base_path,
                            file_dir=f"{set_id.replace(':', '_')}/{binding_id}/mets",
                        ).execute(context={})

                        SaveAltosSFTPOperator(
                            task_id=f"save_altos_{binding_id}",
                            mets_path=f"{base_path}/{set_id.replace(':', '_')}/{binding_id}/mets",
                            sftp_client=sftp_client,
                            ssh_client=ssh_client,
                            tmpdir=TMPDIR,
                            dc_identifier=dc_identifier,
                            base_path=base_path,
                            file_dir=f"{set_id.replace(':', '_')}/{binding_id}/alto",
                        ).execute(context={})

                        ssh_client.exec_command(f"rm -r {TMPDIR}/{binding_id}")

            with open(
                BINDING_BASE_PATH / set_id.replace(":", "_") / "binding_ids", "r"
            ) as f:
                bindings = f.read().splitlines()

            for batch in utils.split_into_batches(bindings):
                download_binding_batch(batch=batch)

        @task(task_id="clear_temp_directory", trigger_rule="all_done")
        def clear_temp_dir():
            ssh_hook = SSHHook(ssh_conn_id=SSH_CONN_ID)
            with ssh_hook.get_conn() as ssh_client:
                ssh_client.exec_command(f"rm -r {TMPDIR}/*")

        (
            begin_download
            >> download_set(
                set_id=set_id, api=api, ssh_conn_id=SSH_CONN_ID, base_path=BASE_PATH
            )
            >> clear_temp_dir()
        )

    download_dag()
