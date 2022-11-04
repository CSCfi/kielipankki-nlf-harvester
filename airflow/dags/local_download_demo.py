"""
#### DAG for downloading METS and ALTO files for a single binding.
"""

from datetime import timedelta
import os

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator

from harvester.file import File
from harvester.mets import METS
from harvester.pmh_interface import PMH_API


DC_IDENTIFIER = "https://digi.kansalliskirjasto.fi/sanomalehti/binding/379973"
API_URL = "https://digi.kansalliskirjasto.fi/interfaces/OAI-PMH"
METS_PATH = "/opt/airflow/downloads/mets"


default_args = {
    "owner": "Kielipankki",
    "start_date": "2022-10-01",
    "retries": 5,
    "retry_delay": timedelta(seconds=30),
}


def save_mets_for_id():
    api = PMH_API(url=API_URL)
    api.fetch_mets(DC_IDENTIFIER, folder_path=METS_PATH)


def download_alto_files():
    for file in os.listdir(METS_PATH):
        path = os.path.join(METS_PATH, file)
        mets = METS(DC_IDENTIFIER, mets_path=path)
        mets.download_alto_files(base_path="/opt/airflow/downloads")


with DAG(
    dag_id="download_altos_for_binding_to_local",
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    doc_md=__doc__,
) as dag:

    start = DummyOperator(task_id="start")

    fetch_mets_for_binding = PythonOperator(
        task_id="save_mets_for_binding", python_callable=save_mets_for_id, dag=dag
    )

    download_alto_files_for_mets = PythonOperator(
        task_id="download_alto_files_for_mets",
        python_callable=download_alto_files,
        dag=dag,
    )

    success = DummyOperator(task_id="success")

    start >> fetch_mets_for_binding >> download_alto_files_for_mets >> success
