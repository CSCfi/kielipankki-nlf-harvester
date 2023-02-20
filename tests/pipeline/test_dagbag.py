"""
POC on how DAGs and their components can be tested
"""

from airflow.models import DagBag
import pytest


@pytest.fixture()
def dagbag():
    """
    DagBag containing the DAGs under pipeline/dags/.
    """
    return DagBag(dag_folder="pipeline/dags")


# Pylint does not understand fixtures
# pylint: disable=redefined-outer-name


@pytest.mark.skip(reason="DAG depends on the Pouta machine file system for now")
def test_dag_loading(dagbag):
    """
    Check that all DAGs can be parsed from the dagbag
    """
    dag = dagbag.get_dag(dag_id="parallel_download")
    assert dag is not None
