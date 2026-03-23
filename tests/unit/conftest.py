"""
Unit-test conftest.py

Provides the ``mock_variables`` fixture for tests that exercise code importing
``airflow.models.Variable``.  Skips the whole module when Airflow is not
installed so that ``test_client.py`` (pure Python) is never blocked.
"""
from __future__ import annotations

import pytest

try:
    import airflow  # noqa: F401

    _AIRFLOW_AVAILABLE = True
except ModuleNotFoundError:
    _AIRFLOW_AVAILABLE = False


def pytest_collection_modifyitems(items):
    """
    Mark tests that use the ``mock_variables`` fixture as skip when Airflow
    is absent.  ``test_client.py`` does not use that fixture so it is unaffected.
    """
    if _AIRFLOW_AVAILABLE:
        return
    skip_mark = pytest.mark.skip(reason="apache-airflow not installed")
    for item in items:
        if "mock_variables" in getattr(item, "fixturenames", []):
            item.add_marker(skip_mark)


@pytest.fixture(scope="session", autouse=True)
def _init_airflow_db():
    """Initialise a throw-away SQLite Airflow DB once per session (no-op if absent)."""
    if not _AIRFLOW_AVAILABLE:
        return
    from airflow.utils.db import initdb

    initdb()


@pytest.fixture
def mock_variables(monkeypatch):
    """
    Patch ``airflow.models.Variable.get`` so tests never hit the database.
    Returns the variable dict so individual tests can override values.
    """
    variables: dict = {
        "COMTRADE_S3_BUCKET": "test-bucket",
        "COMTRADE_TYPE_CODE": "C",
        "COMTRADE_FREQ_CODE": "A",
        "COMTRADE_CL_CODE": "HS",
        "COMTRADE_REPORTER_CODE": None,
        "COMTRADE_PERIOD": None,
        "COMTRADE_PARTNER_CODE": None,
        "COMTRADE_CMD_CODE": None,
        "COMTRADE_FLOW_CODE": None,
        "COMTRADE_WRITE_PARQUET": "false",
        "AWS_ACCESS_KEY_ID": "AKIATEST000000000000",
        "AWS_SECRET_ACCESS_KEY": "testsecretkey0000000000000000000000000000",
        "AWS_DEFAULT_REGION": "us-east-1",
        "COMTRADE_MBS_SERIES_TYPE": "T35",
    }

    def _fake_get(key, default_var=None, **kwargs):
        return variables.get(key, default_var)

    monkeypatch.setattr("airflow.models.Variable.get", staticmethod(_fake_get))
    return variables
