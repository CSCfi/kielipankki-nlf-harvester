import pytest

import pipeline.dags.local_download_demo

from airflow.models import DagBag


@pytest.fixture()
def dagbag():
    """
    DagBag containing the DAGs under pipeline/dags/.
    """
    return DagBag(dag_folder="pipeline/dags")


# Pylint does not understand fixtures
# pylint: disable=redefined-outer-name


def test_dag_loading(dagbag):
    """
    Check that DAG can be parsed from the file
    """
    dag = dagbag.get_dag(dag_id="download_altos_for_binding_to_local")
    assert dag is not None
