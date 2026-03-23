"""
DAG-integrity conftest.py

Initialises the Airflow DB once per session.
Tests that require Airflow are skipped automatically when it is absent.
"""
from __future__ import annotations

import pytest

try:
    import airflow  # noqa: F401

    _AIRFLOW_AVAILABLE = True
except ModuleNotFoundError:
    _AIRFLOW_AVAILABLE = False


def pytest_collection_modifyitems(items):
    if _AIRFLOW_AVAILABLE:
        return
    skip_mark = pytest.mark.skip(reason="apache-airflow not installed")
    for item in items:
        if item.fspath and "dag_integrity" in str(item.fspath):
            item.add_marker(skip_mark)


@pytest.fixture(scope="session", autouse=True)
def _init_airflow_db():
    if not _AIRFLOW_AVAILABLE:
        return
    from airflow.utils.db import initdb

    initdb()
