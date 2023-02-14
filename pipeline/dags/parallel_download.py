"""
Depth-first download procedure
"""

from datetime import timedelta
from requests.exceptions import HTTPError
import os

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.hooks.base import BaseHook
from airflow.decorators import task, task_group
from airflow.utils.task_group import TaskGroup

from harvester.pmh_interface import PMH_API
from harvester import utils

from operators.custom_operators import (
    SaveMetsSFTPOperator,
    SaveAltosForMetsSFTPOperator,
    CreateConnectionOperator,
)

SET_ID = "col-681"
BASE_PATH = "/scratch/project_2006633/nlf-harvester/downloads"
SSH_CONN_ID = "puhti_conn"
HTTP_CONN_ID = "nlf_http_conn"

default_args = {
    "owner": "Kielipankki",
    "start_date": "2022-10-01",
    "retry_delay": timedelta(seconds=10),
}


def list_collections(http_conn_id):
    http_conn = BaseHook.get_connection(http_conn_id)
    api = PMH_API(url=http_conn.host)
    return api.set_ids()


def list_dc_identifiers(http_conn_id, set_id):
    if not os.path.isfile(
        f"/home/ubuntu/airflow/plugins/bindings_{set_id.replace(':', '_')}"
    ):
        http_conn = BaseHook.get_connection(http_conn_id)
        api = PMH_API(url=http_conn.host)
        while True:
            try:
                dc_identifiers = api.dc_identifiers(set_id)
            except HTTPError:
                continue
            else:
                break
        with open(
            f"/home/ubuntu/airflow/plugins/bindings_{set_id.replace(':', '_')}", "w"
        ) as file:
            for dc in dc_identifiers:
                file.write(f"{dc}\n")


list_dc_identifiers(HTTP_CONN_ID, SET_ID)


def download_set(dag: DAG, set_id) -> TaskGroup:
    with TaskGroup(group_id=f"download_set") as download:

        http_conn = BaseHook.get_connection(HTTP_CONN_ID)
        api = PMH_API(url=http_conn.host)

        with open(
            f"/home/ubuntu/airflow/plugins/bindings_{set_id.replace(':', '_')}"
        ) as file:
            dc_identifiers = file.readlines()

        for dc_identifier in dc_identifiers:
            binding_id = utils.binding_id_from_dc(dc_identifier)

            @task(task_id=f"download_binding_{binding_id}", task_group=download)
            def download_binding(dc_identifier):

                ssh_hook = SSHHook(ssh_conn_id=SSH_CONN_ID)
                with ssh_hook.get_conn() as ssh_client:
                    sftp_client = ssh_client.open_sftp()

                    utils.make_intermediate_dirs(
                        sftp_client=sftp_client,
                        remote_directory=f"{BASE_PATH}/{set_id.replace(':', '_')}/{binding_id}/mets",
                    )

                    SaveMetsSFTPOperator(
                        task_id=f"save_mets_{binding_id}",
                        api=api,
                        sftp_client=sftp_client,
                        dc_identifier=dc_identifier,
                        base_path=BASE_PATH,
                        file_dir=f"{set_id.replace(':', '_')}/{binding_id}/mets",
                    ).execute(context={})

                    SaveAltosForMetsSFTPOperator(
                        task_id=f"save_altos_{binding_id}",
                        sftp_client=sftp_client,
                        base_path=BASE_PATH,
                        file_dir=f"{set_id.replace(':', '_')}/{binding_id}/alto",
                        mets_path=f"{BASE_PATH}/{set_id.replace(':', '_')}/{binding_id}/mets",
                        dc_identifier=dc_identifier,
                    ).execute(context={})

            download_binding(dc_identifier)

    return download


with DAG(
    dag_id="parallel_download",
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

    download = download_set(dag, SET_ID)

    start >> create_nlf_connection >> check_api_availability >> download
