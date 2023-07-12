"""
Depth-first parallelized download procedure that creates a DAG for each collection.
Collections are split into chunks and downloaded into disk images.
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
)
from harvester.pmh_interface import PMH_API

INITIAL_DOWNLOAD = True

BASE_PATH = Path("/scratch/project_2006633/nlf-harvester/images")
TMPDIR = Path("/local_scratch/robot_2006633_puhti/harvester")
IMAGE_SPLIT_DIR = Path("/home/ubuntu/image_split/")
BINDING_BASE_PATH = Path("/home/ubuntu/binding_ids_all")
SSH_CONN_ID = "puhti_conn"
HTTP_CONN_ID = "nlf_http_conn"
COLLECTIONS = [
    {"id": "col-361", "image_size": 150},
    {"id": "col-501", "image_size": 5000},
    {"id": "col-82", "image_size": 150000},
    {"id": "col-24", "image_size": 100000},
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

    current_dag_id = f"image_download_{col['id']}"

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
            binding_base_path=BINDING_BASE_PATH,
            http_conn_id=HTTP_CONN_ID,
        ) >> [
            begin_download,
            cancel_pipeline,
        ]

        (
            begin_download
            >> download_set(
                set_id=col["id"],
                image_size=col["image_size"],
                api=api,
                ssh_conn_id=SSH_CONN_ID,
                initial_download=INITIAL_DOWNLOAD,
                image_split_dir=IMAGE_SPLIT_DIR,
                binding_base_path=BINDING_BASE_PATH,
                base_path=BASE_PATH,
                tmpdir=TMPDIR,
            )
            >> clear_temporary_directory(SSH_CONN_ID, TMPDIR)
        )

    download_dag()
