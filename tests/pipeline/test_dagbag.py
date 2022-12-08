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


def test_dag_loading(dagbag):
    """
    Check that all DAGs can be parsed from the dagbag
    """
    dag = dagbag.get_dag(dag_id="download_altos_for_binding_to_local")
    assert dag is not None

    dag_2 = dagbag.get_dag(dag_id="download_set_to_puhti")
    assert dag_2 is not None

    dag_3 = dagbag.get_dag(dag_id="download_all_sets_to_puhti")
    assert dag_3 is not None
