"""
Depth-first download procedure
"""

from datetime import timedelta
import os

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.hooks.base import BaseHook
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup

from harvester.pmh_interface import PMH_API
from harvester import utils

from operators.custom_operators import (
    SaveMetsSFTPOperator,
    SaveAltosSFTPOperator,
    CreateConnectionOperator,
)

BASE_PATH = "/scratch/project_2006633/nlf-harvester/downloads"
SSH_CONN_ID = "puhti_conn"
HTTP_CONN_ID = "nlf_http_conn"
SET_IDS = ["col-681", "col-361"]

default_args = {
    "owner": "Kielipankki",
    "start_date": "2022-10-01",
    "retry_delay": timedelta(seconds=10),
}


def list_dc_identifiers(http_conn_id, set_id):
    """
    A hopefully temporary helper function to save DC identifiers to file for
    later reading while rate limits are an issue.
    """
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


# Again, temporary until rate limits are no longer an issue
for set_id in SET_IDS:
    list_dc_identifiers(HTTP_CONN_ID, set_id)


def download_set(dag: DAG, set_id, api, ssh_conn_id, base_path) -> TaskGroup:
    """
    TaskGroupFactory for downloading METS and ALTOs for one binding.
    """
    with TaskGroup(group_id=f"download_set_{set_id.replace(':', '_')}") as download:

        # Temporary solution while rate limits are an issue:
        with open(
            f"/home/ubuntu/airflow/plugins/bindings_{set_id.replace(':', '_')}"
        ) as file:
            dc_identifiers = file.read().splitlines()

        # This is how it would preferrably be done:
        # expand() only accepts lists or dicts, so dc_identifiers need to be
        # converted to a list. With the current implementation, this would
        # be an issue for large sets, but we have an upcoming ticket to
        # download huge sets in batches anyway, which should hopefully fix
        # this issue.
        # dc_identifiers = list(api.dc_identifiers(set_id))

        @task(
            task_id="download_binding", task_group=download, trigger_rule="none_skipped"
        )
        def download_binding(dc_identifier):
            binding_id = utils.binding_id_from_dc(dc_identifier)
            ssh_hook = SSHHook(ssh_conn_id=ssh_conn_id)
            with ssh_hook.get_conn() as ssh_client:
                sftp_client = ssh_client.open_sftp()

                utils.make_intermediate_dirs(
                    sftp_client=sftp_client,
                    remote_directory=f"{base_path}/{set_id.replace(':', '_')}/{binding_id}/mets",
                )

                SaveMetsSFTPOperator(
                    task_id=f"save_mets_{binding_id}",
                    api=api,
                    sftp_client=sftp_client,
                    dc_identifier=dc_identifier,
                    base_path=base_path,
                    file_dir=f"{set_id.replace(':', '_')}/{binding_id}/mets",
                ).execute(context={})

                SaveAltosSFTPOperator(
                    task_id=f"save_altos_{binding_id}",
                    sftp_client=sftp_client,
                    base_path=base_path,
                    file_dir=f"{set_id.replace(':', '_')}/{binding_id}/alto",
                    mets_path=f"{base_path}/{set_id.replace(':', '_')}/{binding_id}/mets",
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

    create_nlf_connection = CreateConnectionOperator(
        task_id="create_nlf_connection",
        conn_id="nlf_http_conn",
        conn_type="HTTP",
        host="https://digi.kansalliskirjasto.fi/interfaces/OAI-PMH",
        schema="HTTPS",
    )

    begin_download = EmptyOperator(task_id="begin_download")

    cancel_pipeline = EmptyOperator(task_id="cancel_pipeline")

    @task.branch(task_id="check_api_availability")
    def check_api_availability():
        """
        Check if API is responding and download can begin. If not, cancel pipeline.
        """
        api_ok = HttpSensor(
            task_id="http_sensor", http_conn_id="nlf_http_conn", endpoint="/"
        ).poke(context={})

        if api_ok:
            return "begin_download"
        else:
            return "cancel_pipeline"

    check_api_availability = check_api_availability()

    create_nlf_connection >> check_api_availability >> [begin_download, cancel_pipeline]

    http_conn = BaseHook.get_connection(HTTP_CONN_ID)
    api = PMH_API(url=http_conn.host)

    downloads = []

    # Execute TaskGroup for each collection sequentially (to stick to a depth-first procedure).
    # Otherwise Airflow may decide to run 4 tasks from one collection and 4 tasks from another
    # at the same time.

    # for set_id in api.set_ids():
    for set_id in SET_IDS:
        download_tg = download_set(
            dag=dag,
            set_id=set_id,
            api=api,
            ssh_conn_id=SSH_CONN_ID,
            base_path=BASE_PATH,
        )
        if not downloads:
            begin_download >> download_tg
        else:
            downloads[-1] >> download_tg
        downloads.append(download_tg)
