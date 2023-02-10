"""
Depth-first download procedure
"""

from datetime import timedelta
import paramiko

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.hooks.base import BaseHook
from airflow.decorators import task, task_group
from airflow.utils.task_group import TaskGroup

from harvester.mets import METS
from harvester.file import ALTOFile
from harvester.pmh_interface import PMH_API
from harvester import utils

from operators.custom_operators import (
    SaveMetsSFTPOperator,
    SaveAltosForMetsSFTPOperator,
    CreateConnectionOperator,
)

paramiko.util.log_to_file("/home/ubuntu/airflow/logs/paramiko_logs/logs.log")

SET_ID = "col-681"
DC_IDENTIFIER = "https://digi.kansalliskirjasto.fi/sanomalehti/binding/379973"
BASE_PATH = "/scratch/project_2006633/nlf-harvester/downloads"
SSH_CONN_ID = "puhti_conn"
HTTP_CONN_ID = "nlf_http_conn"

default_args = {
    "owner": "Kielipankki",
    "start_date": "2022-10-01",
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}


def list_collections(http_conn_id):
    http_conn = BaseHook.get_connection(http_conn_id)
    api = PMH_API(url=http_conn.host)
    return api.set_ids()


def download_binding(dag: DAG, dc_identifier: str) -> TaskGroup:
    binding_id = utils.binding_id_from_dc(dc_identifier)
    with TaskGroup(group_id=f"download_{binding_id}") as download:

        http_conn = BaseHook.get_connection(HTTP_CONN_ID)
        api = PMH_API(url=http_conn.host)
        ssh_hook = SSHHook(ssh_conn_id=SSH_CONN_ID)
        with ssh_hook.get_conn() as ssh_client:
            sftp_client = ssh_client.open_sftp()

            utils.make_intermediate_dirs(
                sftp_client=sftp_client,
                remote_directory=f"{BASE_PATH}/{SET_ID.replace(':', '_')}/{binding_id}/mets",
            )

            save_mets = SaveMetsSFTPOperator(
                task_id=f"save_mets_{binding_id}",
                api=api,
                sftp_client=sftp_client,
                dc_identifier=dc_identifier,
                base_path=BASE_PATH,
                file_dir=f"{SET_ID.replace(':', '_')}/{binding_id}/mets",
            )

            save_altos = SaveAltosForMetsSFTPOperator(
                task_id=f"save_altos_{binding_id}",
                sftp_client=sftp_client,
                base_path=BASE_PATH,
                file_dir=f"{SET_ID.replace(':', '_')}/{binding_id}/alto",
                mets_path=f"{BASE_PATH}/{SET_ID.replace(':', '_')}/{binding_id}/mets",
                dc_identifier=dc_identifier,
            )
        
            save_mets >> save_altos
    
    return download


with DAG(
    dag_id="parallel_download",
    schedule="@once",
    catchup=False,
    default_args=default_args,
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")

    create_nlf_connection = CreateConnectionOperator(
        task_id="create_nlf_connection",
        conn_id="nlf_http_conn",
        conn_type="HTTP",
        host="https://digi.kansalliskirjasto.fi/interfaces/OAI-PMH",
        schema="HTTPS",
    )

    check_api_availability = HttpSensor(
        task_id="check_api_availability", http_conn_id="nlf_http_conn", endpoint="/"
    )

    download = download_binding(dag, DC_IDENTIFIER)

    start >> create_nlf_connection >> check_api_availability >> download
