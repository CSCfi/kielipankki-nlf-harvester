"""
#### DAG for downloading METS and ALTO files for a single binding to a remote server (Puhti).
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.hooks.base import BaseHook
from airflow.decorators import task

from harvester import utils
from harvester.pmh_interface import PMH_API

from operators.custom_operators import (
    SaveMetsSFTPOperator,
    SaveAltosSFTPOperator,
    CreateConnectionOperator,
)

# DC_IDENTIFIER = "https://digi.kansalliskirjasto.fi/sanomalehti/binding/4699" # this fails
DC_IDENTIFIER = (
    "https://digi.kansalliskirjasto.fi/sanomalehti/binding/379973"  # this doesn't fail
)
BASE_PATH = "/scratch/project_2006633/nlf-harvester/downloads/temp_test/"
TMPDIR = "/local_scratch/robot_2006633_puhti"
SET_ID = "col-681"
# SET_ID = "sanomalehti"
SSH_CONN_ID = "puhti_conn"
HTTP_CONN_ID = "nlf_http_conn"


default_args = {
    "owner": "Kielipankki",
    "start_date": "2022-10-01",
    "retries": 5,
    "retry_delay": timedelta(seconds=10),
}


with DAG(
    dag_id="download_binding_to_puhti",
    schedule_interval="@once",
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

    http_conn = BaseHook.get_connection(HTTP_CONN_ID)
    api = PMH_API(url=http_conn.host)

    @task(task_id="download_binding")
    def download_binding(dc_identifier):
        binding_id = utils.binding_id_from_dc(dc_identifier)
        ssh_hook = SSHHook(ssh_conn_id=SSH_CONN_ID)
        with ssh_hook.get_conn() as ssh_client:
            sftp_client = ssh_client.open_sftp()

            SaveMetsSFTPOperator(
                task_id=f"save_mets_{binding_id}",
                api=api,
                sftp_client=sftp_client,
                ssh_client=ssh_client,
                tmpdir=TMPDIR,
                dc_identifier=dc_identifier,
                base_path=BASE_PATH,
                file_dir=f"{SET_ID.replace(':', '_')}/{binding_id}/mets",
            ).execute(context={})

            SaveAltosSFTPOperator(
                task_id=f"save_altos_{binding_id}",
                sftp_client=sftp_client,
                ssh_client=ssh_client,
                tmpdir=TMPDIR,
                base_path=BASE_PATH,
                file_dir=f"{SET_ID.replace(':', '_')}/{binding_id}/alto",
                mets_path=f"{BASE_PATH}/{SET_ID.replace(':', '_')}/{binding_id}/mets",
                dc_identifier=dc_identifier,
            ).execute(context={})

    success = EmptyOperator(task_id="success")

    (
        start
        >> create_nlf_connection
        >> check_api_availability
        >> download_binding(DC_IDENTIFIER)
        >> success
    )
