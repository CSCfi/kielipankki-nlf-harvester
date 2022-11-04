"""
#### DAG for downloading METS and ALTO files for a single binding to a remote server (Puhti).
"""

from datetime import timedelta
import os

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.hooks.base import BaseHook
from airflow.models import Connection
from airflow import settings

from harvester.file import ALTOFile
from harvester.mets import METS
from harvester.pmh_interface import PMH_API

DC_IDENTIFIER = "https://digi.kansalliskirjasto.fi/sanomalehti/binding/379973"
BASE_PATH = "/scratch/project_2006633/nlf-harvester/downloads"
METS_PATH = "/opt/airflow/downloads/mets"


default_args = {
    "owner": "Kielipankki",
    "start_date": "2022-10-01",
    "retries": 5,
    "retry_delay": timedelta(seconds=10),
}


def create_nlf_conn(conn_id):
    session = settings.Session()
    conn_ids = [conn.conn_id for conn in session.query(Connection).all()]
    if conn_id not in conn_ids:
        conn = Connection(
            conn_id=conn_id,
            conn_type="HTTP",
            host="digi.kansalliskirjasto.fi/interfaces/OAI-PMH",
            schema="HTTPS",
        )
        session.add(conn)
        session.commit()


def save_mets_for_id(http_conn_id):
    http_conn = BaseHook.get_connection(http_conn_id)
    api = PMH_API(url=http_conn.host)
    api.fetch_mets(DC_IDENTIFIER, folder_path=METS_PATH)


def save_alto_files(ssh_conn_id):
    ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)
    with ssh_hook.get_conn() as ssh_client:
        sftp_client = ssh_client.open_sftp()
        
        for file in os.listdir(METS_PATH):
            path = os.path.join(METS_PATH, file)
            mets = METS(DC_IDENTIFIER, mets_path=path)
            alto_files = mets.files_of_type(ALTOFile)

            for alto_file in alto_files:
                alto_file.download(
                    alto_file.download_to_remote,
                    sftp_client=sftp_client,
                    base_path=BASE_PATH,
                )


with DAG(
    dag_id="download_altos_for_binding_to_puhti",
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    doc_md=__doc__,
) as dag:

    start = DummyOperator(task_id="start")

    create_nlf_connection = PythonOperator(
        task_id="create_nlf_connection",
        python_callable=create_nlf_conn,
        op_kwargs={"conn_id": "nlf_http_conn"},
    )

    check_api_availability = HttpSensor(
        task_id="check_api_availability", http_conn_id="nlf_http_conn", endpoint="/"
    )

    save_mets_for_binding = PythonOperator(
        task_id="save_mets_for_binding",
        python_callable=save_mets_for_id,
        op_kwargs={"http_conn_id": "nlf_http_conn", "ssh_conn_id": "puhti_conn"},
        dag=dag,
    )

    save_alto_files_for_mets = PythonOperator(
        task_id="save_alto_files_for_mets",
        python_callable=save_alto_files,
        op_kwargs={"ssh_conn_id": "puhti_conn"},
        dag=dag,
    )

    success = DummyOperator(task_id="success")

    (
        start
        >> create_nlf_connection
        >> check_api_availability
        >> save_mets_for_binding
        >> save_alto_files_for_mets
        >> success
    )
