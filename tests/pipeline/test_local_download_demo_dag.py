"""
POC on how DAGs and their components can be tested
"""

import os

from airflow.models import DagBag
import pytest

from harvester import utils
import pipeline.dags.local_download_demo
from pipeline.plugins.operators.custom_operators import (
    SaveMetsOperator,
    CreateConnectionOperator,
)


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


def test_save_mets_operator(mets_dc_identifier, expected_mets_response, tmp_path):
    """
    Check that executing save_mets_operator does indeed fetch a METS file
    """

    create_nlf_connection = CreateConnectionOperator(
        task_id="create_nlf_connection",
        conn_id="nlf_http_conn",
        conn_type="HTTP",
        host="https://digi.kansalliskirjasto.fi/interfaces/OAI-PMH",
        schema="HTTPS",
    )

    create_nlf_connection.execute(context={})

    operator = SaveMetsOperator(
        task_id="test_save_mets_task",
        http_conn_id="nlf_http_conn",
        dc_identifier=mets_dc_identifier,
        base_path=tmp_path,
    )

    operator.execute(context={})

    binding_id = utils.binding_id_from_dc(mets_dc_identifier)
    mets_location = tmp_path / "mets" / f"{binding_id}_METS.xml"

    assert os.path.exists(mets_location)
    with open(mets_location, "r", encoding="utf-8") as saved_mets:
        assert saved_mets.read() == expected_mets_response
