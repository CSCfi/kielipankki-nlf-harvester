"""
Depth-first download procedure
"""

from datetime import timedelta
import os
import logging
from http.client import HTTPConnection

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

HTTPConnection.debuglevel = 1

logging.basicConfig(filename="/home/ubuntu/airflow/logs/request_logs/request_logs.log")
logging.getLogger().setLevel(logging.DEBUG)
requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.DEBUG)
requests_log.propagate = True

BASE_PATH = "/scratch/project_2006633/nlf-harvester/downloads"
SSH_CONN_ID = "puhti_conn"
HTTP_CONN_ID = "nlf_http_conn"
SET_IDS = ["col-681", "col-361"]

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
        dc_identifiers = api.dc_identifiers(set_id)
        with open(
            f"/home/ubuntu/airflow/plugins/bindings_{set_id.replace(':', '_')}", "w"
        ) as file:
            for dc in dc_identifiers:
                file.write(f"{dc}\n")


for set_id in SET_IDS:
    list_dc_identifiers(HTTP_CONN_ID, set_id)


def download_set(dag: DAG, set_id) -> TaskGroup:
    with TaskGroup(group_id=f"download_set_{set_id.replace(':', '_')}") as download:

        http_conn = BaseHook.get_connection(HTTP_CONN_ID)
        api = PMH_API(url=http_conn.host)

        with open(
            f"/home/ubuntu/airflow/plugins/bindings_{set_id.replace(':', '_')}"
        ) as file:
            dc_identifiers = file.read().splitlines()

        # dc_identifiers = list(api.dc_identifiers(set_id))

        @task(task_id=f"download_binding", task_group=download)
        def download_binding(dc_identifier):
            binding_id = utils.binding_id_from_dc(dc_identifier)
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

        download_binding.expand(dc_identifier=dc_identifiers)

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

    start >> create_nlf_connection >> check_api_availability

    downloads = []

    # Execute TaskGroup for each collection sequentially (to stick to a depth-first procedure).
    # Otherwise Airflow may decide to run 4 tasks from one collection and 4 tasks from another
    # at the same time.
    for set_id in SET_IDS:
        download_tg = download_set(dag, set_id)
        if not downloads:
            check_api_availability >> download_tg
        else:
            prev_tg = downloads[-1]
            prev_tg >> download_tg
        downloads.append(download_tg)
