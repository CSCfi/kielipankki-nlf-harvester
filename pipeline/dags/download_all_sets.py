"""
Download all METS and ALTO files from NLF to Puhti.
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.sensors.http import HttpSensor

from operators.custom_operators import (
    SaveMetsForAllSetsSFTPOperator,
    SaveAltosForAllSetsSFTPOperator,
    CreateConnectionOperator,
)


BASE_PATH = "/scratch/project_2006633/nlf-harvester/nlf_collections"

default_args = {
    "owner": "Kielipankki",
    "start_date": "2022-10-01",
    "retries": 4,
    "retry_delay": timedelta(minutes=10),
    "email": ["helmiina.hotti@csc.fi"],
    "email_on_failure": True
}


with DAG(
    dag_id="download_all_sets_to_puhti",
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

    save_mets_for_all_sets = SaveMetsForAllSetsSFTPOperator(
        task_id="save_mets_for_all_sets",
        http_conn_id="nlf_http_conn",
        ssh_conn_id="puhti_conn",
        base_path=BASE_PATH,
    )

    save_all_alto_files = SaveAltosForAllSetsSFTPOperator(
        task_id="save_all_alto_files",
        http_conn_id="nlf_http_conn",
        ssh_conn_id="puhti_conn",
        base_path=BASE_PATH,
    )

    success = EmptyOperator(task_id="success")

    (
        start
        >> create_nlf_connection
        >> check_api_availability
        >> save_mets_for_all_sets
        >> save_all_alto_files
        >> success
    )
