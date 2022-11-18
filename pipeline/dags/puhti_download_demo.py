"""
#### DAG for downloading METS and ALTO files for a single binding to a remote server (Puhti).
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.http.sensors.http import HttpSensor

from operators.custom_operators import (
    SaveMetsSFTPOperator,
    SaveAltosForMetsSFTPOperator,
    CreateConnectionOperator,
)

DC_IDENTIFIER = "https://digi.kansalliskirjasto.fi/sanomalehti/binding/379973"
BASE_PATH = "/scratch/project_2006633/nlf-harvester/downloads"


default_args = {
    "owner": "Kielipankki",
    "start_date": "2022-10-01",
    "retries": 5,
    "retry_delay": timedelta(seconds=10),
}


with DAG(
    dag_id="download_altos_for_binding_to_puhti",
    schedule_interval="@once",
    catchup=False,
    default_args=default_args,
    doc_md=__doc__,
) as dag:

    start = DummyOperator(task_id="start")

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

    save_mets_for_binding = SaveMetsSFTPOperator(
        task_id="save_mets_for_binding",
        http_conn_id="nlf_http_conn",
        ssh_conn_id="puhti_conn",
        dc_identifier=DC_IDENTIFIER,
        base_path=BASE_PATH,
    )

    save_alto_files_for_mets = SaveAltosForMetsSFTPOperator(
        task_id="save_altos_for_binding",
        http_conn_id="nlf_http_conn",
        ssh_conn_id="puhti_conn",
        base_path=BASE_PATH,
        dc_identifier=DC_IDENTIFIER,
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
