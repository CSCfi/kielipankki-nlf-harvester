import pytest

import pipeline.dags.local_download_demo

from airflow.models import DagBag


@pytest.fixture()
def dagbag():
    return DagBag(dag_folder="pipeline/dags/local_download_demo.py")


def test_dag_loading(dagbag):
    dag = dagbag.get_dag(dag_id="download_altos_for_binding_to_local")
    assert dag is not None
