"""
DAG to fetch binding ids and save them to files.
"""

from datetime import date, timedelta
import os
from pathlib import Path
from sickle.oaiexceptions import NoRecordsMatch

from airflow.models import DagRun, Variable
from airflow.hooks.base import BaseHook
from airflow.decorators import task, task_group, dag
from harvester.pmh_interface import PMH_API

HTTP_CONN_ID = "nlf_http_conn"

default_args = {
    "owner": "Kielipankki",
    "start_date": "2024-01-01",
    "retry_delay": timedelta(minutes=5),
    "retries": Variable.get("retries"),
}


def get_most_recent_dag_run(dag_id):
    dag_runs = DagRun.find(dag_id=dag_id, state="success")
    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    last_run = dag_runs[0].execution_date.strftime("%Y-%m-%d") if dag_runs else None
    return last_run


schedule = Variable.get("schedule")
if schedule == "@monthly":
    # If we are on a monthly schedule, run this task on the last day of the month
    schedule = "0 0 L * *"


@dag(
    dag_id="fetch_binding_ids",
    schedule=schedule,
    default_args=default_args,
    catchup=False,
    doc_md=__doc__,
)
def fetch_bindings_dag():

    http_conn = BaseHook.get_connection(HTTP_CONN_ID)
    api = PMH_API(url=http_conn.host)
    path_config = Variable.get("path_config", deserialize_json=True)

    @task_group(group_id="save_bindings")
    def save_ids():
        @task(task_id="save_ids_for_set")
        def save_ids_for_set(set_id):

            folder_path = Path(
                Variable.get("path_config", deserialize_json=True)["BINDING_LIST_DIR"]
            ) / set_id.replace(":", "_")

            if not os.path.isdir(folder_path):
                os.makedirs(folder_path)

            if Variable.get("initial_download", deserialize_json=True):
                last_run = None
                print("Fetching all bindings for collection {set_id}")
            else:
                last_run = get_most_recent_dag_run(f"subset_download_{set_id}")
                print(f"Fetching bindings for collection {set_id} since {last_run}")

            with open(
                f"{folder_path}/{path_config['ADDED_BINDINGS_PREFIX']}_{date.today()}",
                "w",
            ) as new_bindings_file:

                try:
                    for item in api.dc_identifiers(set_id, from_date=last_run):
                        new_bindings_file.write(item + "\n")
                except NoRecordsMatch:
                    print(f"No new bindings after date {last_run} for set {set_id}")

            with open(
                (
                    f"{folder_path}"
                    "/"
                    f"{path_config['DELETED_BINDINGS_PREFIX']}_{date.today()}"
                ),
                "w",
            ) as deleted_bindings_file:
                for item in api.deleted_dc_identifiers(set_id, from_date=last_run):
                    deleted_bindings_file.write(item + "\n")

        set_ids = [
            collection["id"]
            for collection in Variable.get("collections", deserialize_json=True)
        ]
        save_ids_for_set.expand(set_id=set_ids)

    save_ids()


fetch_bindings_dag()
