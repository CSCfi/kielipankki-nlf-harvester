"""
Depth-first parallelized download procedure that creates a DAG for each collection.
Collections are split into subsets, and further into download batches, and
assembled into targets, currently zip files.
"""

from datetime import date, timedelta
import distutils
from pathlib import Path
import yaml

from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
from airflow.decorators import dag
from airflow.models import Variable
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook

from airflow_slurm.ssh_slurm_operator import SSHSlurmOperator

from includes.tasks import (
    check_if_download_should_begin,
    download_set,
    clear_temporary_directory,
    publish_to_users,
    generate_listings,
)
from harvester.pmh_interface import PMH_API

from datetime import datetime

path_config = Variable.get("path_config", deserialize_json=True)
for path_name in path_config:
    path_config[path_name] = Path(path_config[path_name])

SSH_CONN_ID = "hpc_conn"
HTTP_CONN_ID = "nlf_http_conn"

default_args = {
    "owner": "Kielipankki",
    "start_date": "2024-01-01",
    "retry_delay": timedelta(minutes=5),
    "retries": Variable.get("retries"),
}

http_conn = BaseHook.get_connection(HTTP_CONN_ID)
api = PMH_API(url=http_conn.host)


@dag(
    dag_id="debug_dag",
    schedule=Variable.get("schedule"),
    catchup=False,
    default_args=default_args,
    doc_md=__doc__,
)
def download_dag():
    # ssh_hook = SSHHook(ssh_conn_id=SSH_CONN_ID)

    begin_download = EmptyOperator(task_id="begin_download")

    restic_env = yaml.load(
        open("/home/ubuntu/restic_env.yaml", "r"), Loader=yaml.FullLoader
    )
    slurm_setup_commands = [f'export {k}="{v}"' for k, v in restic_env.items()] + [
        "export CSC_ENV_INIT_NON_INTERACTIVE=yes",
        "source /etc/profile.d/zz-csc-env.sh",
    ]
    slurm_setup_commands.append("export TMPDIR=$LOCAL_SCRATCH")
    slurm_config = Variable.get("slurm_config", deserialize_json=True)
    slurm_log_file_path = f"/users/robot_2006633_roihu/tmp/logs/debugging-{datetime.now().strftime('%Y%m%d-%H%M%S')}.out"
    slurm_debug_task = SSHSlurmOperator(
        task_id="slurm_debug",
        ssh_conn_id=SSH_CONN_ID,
        command="module load allas; restic --help",
        # command=f"restic --help",
        modules=["allas"],
        setup_commands=slurm_setup_commands,
        host_environment_preamble="",
        submit_on_host=True,
        slurm_options={
            "JOB_NAME": "lb_nlf_harvester_backup",
            "OUTPUT_FILE": slurm_log_file_path,
            "TIME": slurm_config["TIME"],
            "NODES": 1,
            "NTASKS": 1,
            "ACCOUNT": "project_2006633",
            "CPUS_PER_TASK": 8,
            "PARTITION": "small",
            "MEM": slurm_config["MEM"],
            "DEADLINE": (date.today() + timedelta(weeks=3)).strftime(
                "%Y-%m-%dT%H:%M:%S"
            ),
            "WRAP": False,
        },
        tdelta_between_checks=5,  # Poll interval (in seconds) for job status
    )

    begin_download >> slurm_debug_task


download_dag()
