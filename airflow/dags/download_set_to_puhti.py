"""
Download all METS and ALTO files from one set to Puhti.
"""

from datetime import timedelta
from more_itertools import peekable
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
from harvester import utils

SET_ID = "col-681"
BASE_PATH = "/scratch/project_2006633/nlf-harvester/downloads"
METS_PATH = f"{BASE_PATH}/mets"


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
            host="https://digi.kansalliskirjasto.fi/interfaces/OAI-PMH",
            schema="HTTPS",
        )
        session.add(conn)
        session.commit()


def save_mets_for_set(http_conn_id, ssh_conn_id):
    http_conn = BaseHook.get_connection(http_conn_id)
    api = PMH_API(url=http_conn.host)

    ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)
    with ssh_hook.get_conn() as ssh_client:
        sftp_client = ssh_client.open_sftp()
        utils.make_intermediate_dirs(
            sftp_client=sftp_client,
            remote_directory=METS_PATH,
        )
        for dc_identifier in api.dc_identifiers(set_id=SET_ID):
            output_file = str(
                utils.construct_mets_download_location(
                    dc_identifier=dc_identifier, base_path=BASE_PATH, file_dir="mets"
                )
            )
            with sftp_client.file(output_file, "w") as file:
                api.download_mets(dc_identifier=dc_identifier, output_mets_file=file)


def save_alto_files(http_conn_id, ssh_conn_id):
    http_conn = BaseHook.get_connection(http_conn_id)
    api = PMH_API(url=http_conn.host)

    ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)
    with ssh_hook.get_conn() as ssh_client:
        sftp_client = ssh_client.open_sftp()

        for dc_identifier in api.dc_identifiers(set_id=SET_ID):
            # NOTE! This expects that all METS files follow the default naming
            # set in utils.construct_mets_download_location. May not be the 
            # most optimal solution.
            path = os.path.join(
                METS_PATH, f"{utils.binding_id_from_dc(dc_identifier)}_METS.xml"
            )
            mets = METS(dc_identifier, sftp_client.file(path, "r"))
            alto_files = peekable(mets.files_of_type(ALTOFile))

            first_alto = alto_files.peek()
            first_alto_path = str(
                utils.construct_file_download_location(
                    file=first_alto, base_path=BASE_PATH
                )
            )
            utils.make_intermediate_dirs(
                sftp_client=sftp_client,
                remote_directory=first_alto_path.rsplit("/", maxsplit=1)[0],
            )
            for alto_file in alto_files:
                output_file = str(
                    utils.construct_file_download_location(
                        file=alto_file, base_path=BASE_PATH
                    )
                )
                with sftp_client.file(output_file, "wb") as file:
                    alto_file.download(
                        output_file=file,
                        chunk_size=1024 * 1024,
                    )


with DAG(
    dag_id="download_set_to_puhti",
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

    save_mets_for_set = PythonOperator(
        task_id="save_mets_for_set",
        python_callable=save_mets_for_set,
        op_kwargs={"http_conn_id": "nlf_http_conn", "ssh_conn_id": "puhti_conn"},
        dag=dag,
    )

    save_alto_files = PythonOperator(
        task_id="save_alto_files",
        python_callable=save_alto_files,
        op_kwargs={"http_conn_id": "nlf_http_conn", "ssh_conn_id": "puhti_conn"},
        dag=dag,
    )

    success = DummyOperator(task_id="success")

    (
        start
        >> create_nlf_connection
        >> check_api_availability
        >> save_mets_for_set
        >> save_alto_files
        >> success
    )
