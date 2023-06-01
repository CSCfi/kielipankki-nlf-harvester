"""
DAG to fetch binding ids and save them to files.
"""

from datetime import timedelta
from pathlib import Path
from datetime import date
from sickle.oaiexceptions import NoRecordsMatch
import os

from airflow.models import DagRun
from airflow.hooks.base import BaseHook
from airflow.decorators import task, task_group, dag
from harvester.pmh_interface import PMH_API


SET_IDS = ["col-361", "col-501", "col-24", "col-82"]
BASE_PATH = Path("/home/ubuntu/binding_ids_all")
HTTP_CONN_ID = "nlf_http_conn"

default_args = {
    "owner": "Kielipankki",
    "start_date": "2022-10-01",
    "retry_delay": timedelta(minutes=5),
    "retries": 3,
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

            folder_path = BASE_PATH / set_id.replace(":", "_")

            if not os.path.isdir(folder_path):
                os.makedirs(folder_path)
            last_run = get_most_recent_dag_run("fetch_binding_ids")

            with open(f"{folder_path}/binding_ids_{date.today()}", "w") as file_obj:

                try:
                    for item in api.dc_identifiers(set_id, from_date=last_run):
                        file_obj.write(item + "\n")
                except NoRecordsMatch:
                    print(f"No new bindings after date {last_run}")

        save_ids_for_set.expand(set_id=SET_IDS)

    save_ids()


fetch_bindings_dag()
