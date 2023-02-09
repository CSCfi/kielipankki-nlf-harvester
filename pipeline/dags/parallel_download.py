"""
Depth-first download procedure
"""

from datetime import timedelta

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

SET_ID = "col-681"
BASE_PATH = "/scratch/project_2006633/nlf-harvester/downloads"
SSH_CONN_ID = "puhti_conn"
HTTP_CONN_ID = "nlf_http_conn"

default_args = {
    "owner": "Kielipankki",
    "start_date": "2022-10-01",
    "retries": 4,
    "retry_delay": timedelta(minutes=10),
    "email": ["helmiina.hotti@csc.fi"],
    "email_on_failure": True,
}


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

    @task(task_id="list_collections")
    def list_collections(http_conn_id):
        http_conn = BaseHook.get_connection(http_conn_id)
        api = PMH_API(url=http_conn.host)
        return api.set_ids()

    collection_tasks = []

    http_conn = BaseHook.get_connection(HTTP_CONN_ID)
    api = PMH_API(url=http_conn.host)

    for set_id in api.set_ids():

        @task_group(group_id="download_set")
        def download_set(set_id):

            ssh_hook = SSHHook(ssh_conn_id=SSH_CONN_ID)
            with ssh_hook.get_conn() as ssh_client:
                sftp_client = ssh_client.open_sftp()

                @task_group(group_id="download_binding")
                def download_binding(set_id, dc_identifier):

                    save_mets = SaveMetsSFTPOperator(
                        api=api,
                        sftp_client=sftp_client,
                        dc_identifier=dc_identifier,
                        base_path=BASE_PATH,
                        file_dir=f"{set_id.replace(':', '_')}/{utils.binding_id_from_dc(dc_identifier)}/mets",
                        task_group=download_binding
                    )

                    save_altos = SaveAltosForMetsSFTPOperator(
                        sftp_client=sftp_client,
                        base_path=BASE_PATH,
                        file_dir=f"{set_id.replace(':', '_')}/{utils.binding_id_from_dc(dc_identifier)}/alto",
                        mets_path=f"{BASE_PATH}/{set_id.replace(':', '_')}/{utils.binding_id_from_dc(dc_identifier)}/mets",
                        dc_identifier=dc_identifier,
                        task_group=download_binding
                    )

                    save_mets >> save_altos

                download_binding.partial(set_id=set_id).expand(dc_identifier=api.dc_identifiers(set_id))

        collection_tasks.append(download_set(set_id=set_id))
    
    start >> create_nlf_connection >> check_api_availability >> collection_tasks
