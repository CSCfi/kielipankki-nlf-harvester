"""
#### DAG for downloading METS and ALTO files for a single binding.
"""

from datetime import timedelta
from pathlib import Path
import os

from airflow import DAG
from airflow.models import BaseOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator

from harvester.mets import METS
from harvester.file import ALTOFile
from harvester.pmh_interface import PMH_API
from harvester import utils


DC_IDENTIFIER = "https://digi.kansalliskirjasto.fi/sanomalehti/binding/379973"
API_URL = "https://digi.kansalliskirjasto.fi/interfaces/OAI-PMH"
BASE_PATH = Path("/opt/airflow/downloads/")
METS_PATH = Path("/opt/airflow/downloads/mets")


default_args = {
    "owner": "Kielipankki",
    "start_date": "2022-10-01",
    "retries": 5,
    "retry_delay": timedelta(seconds=10),
}


class SaveMetsOperator(BaseOperator):
    def __init__(self, api_url, dc_identifier, base_path, **kwargs):
        super().__init__(**kwargs)
        self.api_url = api_url
        self.dc_identifier = dc_identifier
        self.base_path = base_path

    def execute(self, context):
        api = PMH_API(url=self.api_url)
        output_file = utils.construct_mets_download_location(
            dc_identifier=self.dc_identifier,
            base_path=self.base_path,
            file_dir="mets",
        )
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, "wb") as file:
            return api.download_mets(
                dc_identifier=self.dc_identifier, output_mets_file=file
            )


def download_alto_files():
    for file in os.listdir(METS_PATH):
        path = os.path.join(METS_PATH, file)
        mets = METS(DC_IDENTIFIER, open(path, "rb"))
        alto_files = mets.files_of_type(ALTOFile)
        for alto_file in alto_files:
            output_file = utils.construct_file_download_location(
                file=alto_file, base_path=BASE_PATH
            )
            output_file.parent.mkdir(parents=True, exist_ok=True)
            with open(output_file, "wb") as file:
                alto_file.download(output_file=file)


with DAG(
    dag_id="download_altos_for_binding_to_local",
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    doc_md=__doc__,
) as dag:

    start = DummyOperator(task_id="start")

    fetch_mets_for_binding = SaveMetsOperator(
        task_id="save_mets_for_binding",
        dag=dag,
        api_url=API_URL,
        dc_identifier=DC_IDENTIFIER,
        base_path=BASE_PATH,
    )

    download_alto_files_for_mets = PythonOperator(
        task_id="download_alto_files_for_mets",
        python_callable=download_alto_files,
        dag=dag,
    )

    success = DummyOperator(task_id="success")

    start >> fetch_mets_for_binding >> download_alto_files_for_mets >> success
