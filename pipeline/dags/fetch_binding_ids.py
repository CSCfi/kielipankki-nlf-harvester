"""
DAG to fetch binding ids and save them to files.
"""

from datetime import timedelta
from pathlib import Path
import os

from airflow.hooks.base import BaseHook
from airflow.decorators import task, task_group, dag
from harvester.pmh_interface import PMH_API

SET_IDS = ["col-24", "col-82", "col-361", "col-501"]
BASE_PATH = Path("/home/ubuntu/binding_ids_all")
HTTP_CONN_ID = "nlf_http_conn"

default_args = {
    "owner": "Kielipankki",
    "start_date": "2022-10-01",
    "retry_delay": timedelta(minutes=5),
    "retries": 3,
}


@dag(
    dag_id="fetch_binding_ids",
    schedule="@once",
    default_args=default_args,
    catchup=False,
    doc_md=__doc__,
)
def fetch_bindings_dag():

    http_conn = BaseHook.get_connection(HTTP_CONN_ID)
    api = PMH_API(url=http_conn.host)

    @task_group(group_id=f"save_bindings")
    def save_ids():
        @task(task_id=f"save_ids_for_set")
        def save_ids_for_set(set_id):

            folder_path = BASE_PATH / set_id.replace(":", "_")

            if not os.path.isdir(folder_path):
                os.makedirs(folder_path)

            with open(f"{folder_path}/binding_ids", "w") as file_obj:

                for item in api.dc_identifiers(set_id):
                    file_obj.write(item + "\n")

        save_ids_for_set.expand(set_id=SET_IDS)

    save_ids()


fetch_bindings_dag()
