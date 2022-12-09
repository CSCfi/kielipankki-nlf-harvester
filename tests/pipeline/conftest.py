"""
Shared fixtures for pipeline testing
"""

import sys

import pytest


@pytest.fixture(autouse=True, scope="package")
def airflow_path_addition():
    """
    Airflow automatically adds some paths to sys.path: do that manually for tests.

    Without this, our tests won't discover custom operators like production Airflow
    would.
    """
    sys.path.append("pipeline/plugins")
