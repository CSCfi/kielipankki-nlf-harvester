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
    "start_date": "2022-10-01",
    "retry_delay": timedelta(minutes=5),
    "retries": Variable.get("retries"),
}


def get_most_recent_dag_run(dag_id):
    dag_runs = DagRun.find(dag_id=dag_id, state="success")
    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    last_run = dag_runs[0].execution_date.strftime("%Y-%m-%d") if dag_runs else None
    return last_run


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

            folder_path = Path(
                Variable.get("path_config", deserialize_json=True)["BINDING_LIST_DIR"]
            ) / set_id.replace(":", "_")

            if not os.path.isdir(folder_path):
                os.makedirs(folder_path)
            last_run = get_most_recent_dag_run(f"subset_download_{set_id}")

            with open(f"{folder_path}/binding_ids_{date.today()}", "w") as file_obj:

                if Variable.get("initial_download", deserialize_json=True):
                    for item in api.dc_identifiers(set_id):
                        file_obj.write(item + "\n")
                else:
                    try:
                        for item in api.dc_identifiers(set_id, from_date=last_run):
                            file_obj.write(item + "\n")
                    except NoRecordsMatch:
                        print(f"No new bindings after date {last_run} for set {set_id}")

        set_ids = [
            collection["id"]
            for collection in Variable.get("collections", deserialize_json=True)
        ]
        save_ids_for_set.expand(set_id=set_ids)

    save_ids()


fetch_bindings_dag()
