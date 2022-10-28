"""
#### DAG for downloading METS and ALTO files for a single binding to a remote server (Puhti).
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.contrib.hooks.ssh_hook import SSHHook

from harvester.file import ALTOFile
from harvester.mets import METS
from harvester.pmh_interface import PMH_API

DC_IDENTIFIER = "https://digi.kansalliskirjasto.fi/sanomalehti/binding/379973"
BASE_PATH = "/scratch/project_2006633/nlf-harvester/downloads"


default_args = {
    "owner": "Kielipankki",
    "start_date": "2022-10-01",
    "retries": 5,
    "retry_delay": timedelta(seconds=10),
}


def save_mets_for_id(ssh_conn_id):
    api = PMH_API(url="https://digi.kansalliskirjasto.fi/interfaces/OAI-PMH")
    ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)
    with ssh_hook.get_conn() as ssh_client:
        sftp_client = ssh_client.open_sftp()

        mets_path = f"{BASE_PATH}/mets"
        print(f"Downloading METS to {mets_path}")
        api.download_mets_to_remote(
            dc_identifier=DC_IDENTIFIER, folder_path=mets_path, sftp_client=sftp_client
        )
        print("METS downloaded succesfully.")


def save_alto_files(ssh_conn_id):
    ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)
    with ssh_hook.get_conn() as ssh_client:
        sftp_client = ssh_client.open_sftp()
        mets_path = f"{BASE_PATH}/mets"
        mets_files = sftp_client.listdir(mets_path)

        for filename in mets_files:
            with sftp_client.file(f"{mets_path}/{filename}", "r") as remote_mets_file:
                mets_content = remote_mets_file.read()
                mets = METS(DC_IDENTIFIER, mets_content=mets_content)
                alto_files = mets.files_of_type(ALTOFile)

                print(f"Downloading {len(alto_files)} ALTO files to remote.")
                for file in alto_files:
                    file.download_to_remote(
                        sftp_client=sftp_client, base_path=BASE_PATH
                    )
                print("ALTO files downloaded successfully.")


with DAG(
    dag_id="download_altos_for_binding_in_puhti",
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    doc_md=__doc__,
) as dag:

    start = DummyOperator(task_id="start")

    check_api_availability = HttpSensor(
        task_id="check_api_availability", http_conn_id="nlf_http_conn", endpoint="/"
    )

    save_mets_for_binding = PythonOperator(
        task_id="save_mets_for_binding",
        python_callable=save_mets_for_id,
        op_kwargs={"ssh_conn_id": "helmiina_puhti_conn"},
        dag=dag,
    )

    save_alto_files_for_mets = PythonOperator(
        task_id="save_alto_files_for_mets",
        python_callable=save_alto_files,
        op_kwargs={"ssh_conn_id": "helmiina_puhti_conn"},
        dag=dag,
    )

    success = DummyOperator(task_id="success")

    (
        start
        >> check_api_availability
        >> save_mets_for_binding
        >> save_alto_files_for_mets
        >> success
    )