"""
Depth-first parallelized download procedure that creates a DAG for each collection.
Collections are split into chunks and downloaded into disk images.
"""

from datetime import timedelta
from pathlib import Path

from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
from airflow.decorators import dag

from includes.tasks import check_if_download_should_begin, download_set, clear_temp_dir
from harvester.pmh_interface import PMH_API
from harvester import utils

from importlib import reload

reload(utils)


INITIAL_DOWNLOAD = True

BASE_PATH = "/scratch/project_2006633/nlf-harvester/images"
TMPDIR = "/local_scratch/robot_2006633_puhti/harvester-temp"
IMAGE_SPLIT_DIR = Path("/home/ubuntu/image_split/")
BINDING_BASE_PATH = Path("/home/ubuntu/binding_ids_all")
SSH_CONN_ID = "puhti_conn"
HTTP_CONN_ID = "nlf_http_conn"
SET_IDS = ["col-361"]

default_args = {
    "owner": "Kielipankki",
    "start_date": "2023-05-22",
    "retry_delay": timedelta(minutes=5),
    "retries": 3,
}

http_conn = BaseHook.get_connection(HTTP_CONN_ID)
api = PMH_API(url=http_conn.host)


for set_id in SET_IDS:

    current_dag_id = f"image_download_{set_id}"

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
            set_id=set_id,
            binding_base_path=BINDING_BASE_PATH,
            http_conn_id=HTTP_CONN_ID,
        ) >> [
            begin_download,
            cancel_pipeline,
        ]

        (
            begin_download
            >> download_set(
                set_id=set_id,
                api=api,
                ssh_conn_id=SSH_CONN_ID,
                initial_download=INITIAL_DOWNLOAD,
                image_split_dir=IMAGE_SPLIT_DIR,
                binding_base_path=BINDING_BASE_PATH,
                base_path=BASE_PATH,
                tmpdir=TMPDIR,
            )
            >> clear_temp_dir(SSH_CONN_ID, TMPDIR)
        )

    download_dag()
