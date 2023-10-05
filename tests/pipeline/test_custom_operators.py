from airflow import settings
from airflow.models import Connection

from pipeline.plugins.operators.custom_operators import CreateConnectionOperator


def test_create_connection_operator():
    """
    Check that CreateConnectionOperator actually creates an Airflow connection
    """
    create_nlf_connection = CreateConnectionOperator(
        task_id="create_nlf_connection",
        conn_id="nlf_http_conn",
        conn_type="HTTP",
        host="https://digi.kansalliskirjasto.fi/interfaces/OAI-PMH",
        schema="HTTPS",
    )

    create_nlf_connection.execute(context={})

    session = settings.Session()
    conn_ids = [conn.conn_id for conn in session.query(Connection).all()]
    assert "nlf_http_conn" in conn_ids
