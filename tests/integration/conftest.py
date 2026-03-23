"""
Integration test conftest.py

Provides shared fixtures for smoke tests that exercise multiple pipeline
components together against a moto-emulated S3.

All integration tests are skipped when Airflow is not installed (the pipeline
modules import Airflow lazily, but the Variable.get patch requires it).
"""
from __future__ import annotations

import json

import boto3
import pytest

try:
    import airflow  # noqa: F401

    _AIRFLOW_AVAILABLE = True
except ModuleNotFoundError:
    _AIRFLOW_AVAILABLE = False

from moto import mock_aws  # noqa: E402


def pytest_collection_modifyitems(items):
    if _AIRFLOW_AVAILABLE:
        return
    skip_mark = pytest.mark.skip(reason="apache-airflow not installed")
    for item in items:
        if item.fspath and "integration" in str(item.fspath):
            item.add_marker(skip_mark)

BUCKET = "test-bucket"
REGION = "us-east-1"


# ── AWS / Airflow mocking ─────────────────────────────────────────────────────


@pytest.fixture
def mock_variables(monkeypatch):
    """
    Patch Variable.get so integration tests never need a live Airflow DB.
    Returns the variable dict so individual tests can override values.
    """
    variables: dict = {
        "COMTRADE_S3_BUCKET": BUCKET,
        "AWS_ACCESS_KEY_ID": "AKIATEST000000000000",
        "AWS_SECRET_ACCESS_KEY": "testsecretkey0000000000000000000000000000",
        "AWS_DEFAULT_REGION": REGION,
        "COMTRADE_FREQ_CODE": "A",
        "COMTRADE_SLACK_WEBHOOK_URL": None,
        "COMTRADE_WRITE_PARQUET": "false",
        "COMTRADE_WRITE_ICEBERG": "false",
    }

    def _fake_get(key, default_var=None, **kwargs):
        return variables.get(key, default_var)

    monkeypatch.setattr("airflow.models.Variable.get", staticmethod(_fake_get))
    return variables


@pytest.fixture
def aws(mock_variables):
    """
    Start the moto AWS mock, create the test S3 bucket, and yield a boto3 S3
    client.  Torn down automatically when the test exits.
    """
    with mock_aws():
        client = boto3.client("s3", region_name=REGION)
        client.create_bucket(Bucket=BUCKET)
        yield client


# ── Data fixtures ─────────────────────────────────────────────────────────────


@pytest.fixture
def trade_records():
    """Two realistic Comtrade /preview records."""
    return [
        {
            "typeCode": "C",
            "freqCode": "A",
            "period": "2023",
            "reporterCode": "840",
            "reporterISO": "USA",
            "reporterDesc": "United States of America",
            "partnerCode": "276",
            "partnerISO": "DEU",
            "partnerDesc": "Germany",
            "cmdCode": "TOTAL",
            "cmdDesc": "All Commodities",
            "flowCode": "X",
            "flowDesc": "Export",
            "primaryValue": 1_234_567.89,
        },
        {
            "typeCode": "C",
            "freqCode": "A",
            "period": "2023",
            "reporterCode": "276",
            "reporterISO": "DEU",
            "reporterDesc": "Germany",
            "partnerCode": "840",
            "partnerISO": "USA",
            "partnerDesc": "United States of America",
            "cmdCode": "TOTAL",
            "cmdDesc": "All Commodities",
            "flowCode": "M",
            "flowDesc": "Import",
            "primaryValue": 987_654.32,
        },
    ]


@pytest.fixture
def api_envelope(trade_records):
    """Full Comtrade API response envelope wrapping trade_records."""
    return {
        "elapsedMs": 42,
        "count": len(trade_records),
        "data": trade_records,
    }


@pytest.fixture
def bronze_key(aws, api_envelope):
    """
    Write the api_envelope to S3 as a bronze JSON object and return the key.
    Reusable by tests that need a pre-populated bronze layer.
    """
    from comtrade.s3_writer import build_s3_key, write_json_to_s3

    key = build_s3_key(
        endpoint="preview",
        run_id="smoke-test-run",
        year="2023",
        month="01",
        typeCode="C",
        freqCode="A",
    )
    write_json_to_s3(api_envelope, BUCKET, key)
    return key
