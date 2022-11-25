"""
#### DAG for downloading METS and ALTO files for a single binding.
"""

from datetime import timedelta
from pathlib import Path
from airflow import DAG
from airflow.models import BaseOperator
from airflow.operators.dummy import DummyOperator

from operators.custom_operators import (
    SaveMetsOperator,
    SaveAltosOperator,
    CreateConnectionOperator,
)


DC_IDENTIFIER = "https://digi.kansalliskirjasto.fi/sanomalehti/binding/379973"
BASE_PATH = Path("/opt/airflow/downloads/")

default_args = {
    "owner": "Kielipankki",
    "start_date": "2022-10-01",
    "retries": 5,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    dag_id="download_altos_for_binding_to_local",
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

    fetch_mets_for_binding = SaveMetsOperator(
        task_id="save_mets_for_binding",
        http_conn_id="nlf_http_conn",
        dc_identifier=DC_IDENTIFIER,
        base_path=BASE_PATH,
        dag=dag,
    )

    download_alto_files_for_mets = SaveAltosOperator(
        task_id="download_alto_files_for_mets",
        dc_identifier=DC_IDENTIFIER,
        base_path=BASE_PATH,
        dag=dag,
    )

    success = DummyOperator(task_id="success")

    start >> fetch_mets_for_binding >> download_alto_files_for_mets >> success
