"""
Depth-first parallelized download procedure that creates a DAG for each collection.
Collections are split into subsets, and further into download batches, and
assembled into targets, currently zip files.
"""

from datetime import timedelta
import distutils
from pathlib import Path

from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
from airflow.decorators import dag
from airflow.models import Variable

from includes.tasks import (
    check_if_download_should_begin,
    download_set,
    clear_temporary_directory,
    create_restic_snapshot,
    publish_to_users,
    generate_listings,
)
from harvester.pmh_interface import PMH_API

path_config = Variable.get("path_config", deserialize_json=True)
for path_name in path_config:
    path_config[path_name] = Path(path_config[path_name])

SSH_CONN_ID = "puhti_conn"
HTTP_CONN_ID = "nlf_http_conn"

default_args = {
    "owner": "Kielipankki",
    "start_date": "2023-05-22",
    "retry_delay": timedelta(minutes=5),
    "retries": Variable.get("retries"),
}

http_conn = BaseHook.get_connection(HTTP_CONN_ID)
api = PMH_API(url=http_conn.host)


for col in Variable.get("collections", deserialize_json=True):
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

        zip_creation_dir = path_config["OUTPUT_DIR"] / "targets"
        published_data_dir = path_config["OUTPUT_DIR"] / "zip"

        check_if_download_should_begin(
            set_id=col["id"],
            binding_list_dir=path_config["BINDING_LIST_DIR"],
            http_conn_id=HTTP_CONN_ID,
            path_config=path_config,
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
                initial_download=Variable.get(
                    "initial_download", deserialize_json=True
                ),
                path_config=path_config,
            )
            >> clear_temporary_directory(SSH_CONN_ID, path_config["TMPDIR_ROOT"])
            >> publish_to_users(
                ssh_conn_id=SSH_CONN_ID,
                source=zip_creation_dir,
                destination=published_data_dir,
            )
            >> generate_listings(
                ssh_conn_id=SSH_CONN_ID,
                set_id=col["id"],
                published_data_dir=published_data_dir,
                path_config=path_config,
            )
            >> create_restic_snapshot(
                SSH_CONN_ID,
                path_config["EXTRA_BIN_DIR"] / "create_snapshot.sh",
                published_data_dir,
            )
        )

    download_dag()
