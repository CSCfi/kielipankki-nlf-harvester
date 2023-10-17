"""
Depth-first parallelized download procedure that creates a DAG for each collection.
Collections are split into subsets, and further into download batches, and
assembled into targets, currently zip files.
"""

from datetime import timedelta
from pathlib import Path

from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
from airflow.decorators import dag

from includes.tasks import (
    check_if_download_should_begin,
    download_set,
    clear_temporary_directory,
    create_restic_snapshot,
)
from harvester.pmh_interface import PMH_API

INITIAL_DOWNLOAD = True

pathdict = {
    "OUTPUT_DIR": Path("/scratch/project_2006633/nlf-harvester"),
    "TMPDIR_ROOT": Path("/local_scratch/robot_2006633_puhti/harvester"),
    "EXTRA_BIN_DIR": Path("/projappl/project_2006633/local/bin"),
    "SUBSET_SPLIT_DIR": Path("/home/ubuntu/subset_split/"),
    "BINDING_LIST_DIR": Path("/home/ubuntu/binding_ids_all"),
}
SSH_CONN_ID = "puhti_conn"
HTTP_CONN_ID = "nlf_http_conn"
COLLECTIONS = [
    {"id": "col-361", "subset_size": 150},
    {"id": "col-861", "subset_size": 100000},
]

default_args = {
    "owner": "Kielipankki",
    "start_date": "2023-05-22",
    "retry_delay": timedelta(minutes=5),
    "retries": 3,
}

http_conn = BaseHook.get_connection(HTTP_CONN_ID)
api = PMH_API(url=http_conn.host)


for col in COLLECTIONS:

    current_dag_id = f"subset_download_{col['id']}"

    @dag(
        dag_id=current_dag_id,
        schedule="@once",
        catchup=False,
        default_args=default_args,
        doc_md=__doc__,
    )
    def download_dag():
        begin_download = EmptyOperator(task_id="begin_download")

        cancel_pipeline = EmptyOperator(task_id="cancel_pipeline")

        check_if_download_should_begin(
            set_id=col["id"],
            binding_list_dir=pathdict["BINDING_LIST_DIR"],
            http_conn_id=HTTP_CONN_ID,
        ) >> [
            begin_download,
            cancel_pipeline,
        ]

        (
            begin_download
            >> download_set(
                set_id=col["id"],
                subset_size=col["subset_size"],
                api=api,
                ssh_conn_id=SSH_CONN_ID,
                initial_download=INITIAL_DOWNLOAD,
                pathdict=pathdict,
            )
            >> clear_temporary_directory(SSH_CONN_ID, pathdict["TMPDIR_ROOT"])
            >> create_restic_snapshot(
                SSH_CONN_ID,
                pathdict["EXTRA_BIN_DIR"] / "create_snapshot.sh",
                pathdict["OUTPUT_DIR"] / "targets",
            )
        )

    download_dag()
