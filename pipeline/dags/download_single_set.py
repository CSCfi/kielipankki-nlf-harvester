"""
Download all METS and ALTO files from one set to Puhti.
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.sensors.http import HttpSensor

from operators.custom_operators import (
    SaveMetsForSetSFTPOperator,
    SaveAltosForSetSFTPOperator,
    CreateConnectionOperator,
)

SET_ID = "col-681"
BASE_PATH = "/scratch/project_2006633/nlf-harvester/downloads"


default_args = {
    "owner": "Kielipankki",
    "start_date": "2022-10-01",
    "retries": 5,
    "retry_delay": timedelta(seconds=10),
}


with DAG(
    dag_id="download_set_to_puhti",
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

    save_mets_for_set = SaveMetsForSetSFTPOperator(
        task_id="save_mets_for_set",
        http_conn_id="nlf_http_conn",
        ssh_conn_id="puhti_conn",
        base_path=BASE_PATH,
        set_id=SET_ID,
    )

    save_alto_files = SaveAltosForSetSFTPOperator(
        task_id="save_alto_files",
        http_conn_id="nlf_http_conn",
        ssh_conn_id="puhti_conn",
        base_path=BASE_PATH,
        set_id=SET_ID,
    )

    success = EmptyOperator(task_id="success")

    (
        start
        >> create_nlf_connection
        >> check_api_availability
        >> save_mets_for_set
        >> save_alto_files
        >> success
    )
