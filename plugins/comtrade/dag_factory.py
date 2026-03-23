"""
Factory for the three standard tasks every Comtrade DAG contains:
  1. extract_and_store_raw  — call API, write raw JSON to S3 (bronze)
  2. validate_bronze        — run data-quality checks; fail fast on bad data
  3. convert_to_parquet     — (optional) convert bronze JSON to Parquet

Usage in a DAG file:
    from comtrade.dag_factory import make_extract_task, make_validate_task, make_parquet_task
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from airflow.decorators import task
from airflow.models import Variable

from comtrade.s3_writer import build_s3_key, write_json_to_s3, write_parquet_to_s3

logger = logging.getLogger(__name__)


def _bucket() -> str:
    return Variable.get("COMTRADE_S3_BUCKET")


def _parquet_enabled() -> bool:
    return Variable.get("COMTRADE_WRITE_PARQUET", default_var="false").lower() == "true"


def make_extract_task(
    endpoint: str,
    api_fn: Callable,
    api_kwargs_fn: Callable[[], Dict],
    typeCode: Optional[str] = None,
    freqCode: Optional[str] = None,
    extra_partitions: Optional[Dict[str, str]] = None,
):
    """
    Returns an @task function that calls api_fn(**api_kwargs_fn()),
    writes the raw response as JSON to S3, and returns the S3 key.

    api_kwargs_fn is evaluated at runtime so Airflow Variables are resolved lazily.
    """
    @task(task_id="extract_and_store_raw")
    def extract_and_store_raw(**context) -> str:
        run_id = context["run_id"].replace(":", "-").replace("+", "-")
        logical_date: datetime = context["logical_date"]
        year = logical_date.strftime("%Y")
        month = logical_date.strftime("%m")

        data = api_fn(**api_kwargs_fn())

        key = build_s3_key(
            endpoint=endpoint,
            run_id=run_id,
            year=year,
            month=month,
            typeCode=typeCode,
            freqCode=freqCode,
            extra_partitions=extra_partitions,
            fmt="json",
        )
        write_json_to_s3(data, _bucket(), key)
        return key

    return extract_and_store_raw


def make_validate_task(
    endpoint: str,
    required_columns: Optional[List[str]] = None,
    numeric_columns: Optional[List[str]] = None,
    dedup_columns: Optional[List[str]] = None,
    min_rows: int = 1,
    freq_code_variable: Optional[str] = "COMTRADE_FREQ_CODE",
):
    """
    Returns an @task function that reads the bronze JSON from S3, runs the
    full data-quality check suite, and returns the same S3 key on success.

    On failure it raises ``DataQualityError``, which causes Airflow to mark
    the task (and the DAG run) as failed.

    Parameters
    ----------
    endpoint:
        Comtrade endpoint name — used only for log messages.
    required_columns:
        Column names that must be present and non-null in every record.
    numeric_columns:
        Column names that must be >= 0 (WARNING severity).
    dedup_columns:
        Column names forming the natural key — duplicate combinations are
        flagged as warnings.
    min_rows:
        Minimum number of records expected.  Defaults to 1.
    freq_code_variable:
        Airflow Variable name holding the frequency code ("A" or "M") used to
        validate period format.  Set to None to skip period-format check.
    """

    @task(task_id="validate_bronze")
    def validate_bronze(json_key: str, **context) -> str:
        import boto3

        from comtrade.validator import assert_quality, run_checks

        bucket = _bucket()
        freq_code = (
            Variable.get(freq_code_variable, default_var=None)
            if freq_code_variable
            else None
        )

        obj = boto3.client(
            "s3",
            aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID", default_var=None),
            aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY", default_var=None),
            region_name=Variable.get("AWS_DEFAULT_REGION", default_var="us-east-1"),
        ).get_object(Bucket=bucket, Key=json_key)

        data = json.loads(obj["Body"].read())

        logger.info("Running data quality checks for endpoint=%s key=%s", endpoint, json_key)
        results = run_checks(
            data,
            required_columns=required_columns,
            numeric_columns=numeric_columns,
            dedup_columns=dedup_columns,
            min_rows=min_rows,
            freq_code=freq_code,
        )
        assert_quality(results)

        passed = sum(1 for r in results if r.passed)
        logger.info(
            "Quality gate passed for %s — %d/%d checks OK", endpoint, passed, len(results)
        )
        return json_key  # pass-through: parquet task uses this key

    return validate_bronze


def make_parquet_task(
    endpoint: str,
    typeCode: Optional[str] = None,
    freqCode: Optional[str] = None,
    extra_partitions: Optional[Dict[str, str]] = None,
):
    """
    Returns an @task function that reads the JSON written by the extract task
    from S3, converts the 'data' array to Parquet, and uploads it.
    Skipped when COMTRADE_WRITE_PARQUET != 'true'.
    """
    @task(task_id="convert_to_parquet")
    def convert_to_parquet(json_key: str, **context) -> Optional[str]:
        if not _parquet_enabled():
            logger.info("Parquet conversion disabled (COMTRADE_WRITE_PARQUET != true). Skipping.")
            return None

        import boto3
        logical_date: datetime = context["logical_date"]
        run_id = context["run_id"].replace(":", "-").replace("+", "-")
        year = logical_date.strftime("%Y")
        month = logical_date.strftime("%m")

        bucket = _bucket()
        obj = boto3.client(
            "s3",
            aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID", default_var=None),
            aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY", default_var=None),
            region_name=Variable.get("AWS_DEFAULT_REGION", default_var="us-east-1"),
        ).get_object(Bucket=bucket, Key=json_key)
        raw = json.loads(obj["Body"].read())

        # The Comtrade API wraps records in a 'data' key
        records = raw.get("data", raw) if isinstance(raw, dict) else raw
        if not isinstance(records, list) or not records:
            logger.warning("No records to convert for key %s", json_key)
            return None

        parquet_key = build_s3_key(
            endpoint=endpoint,
            run_id=run_id,
            year=year,
            month=month,
            typeCode=typeCode,
            freqCode=freqCode,
            extra_partitions=extra_partitions,
            fmt="parquet",
        )
        return write_parquet_to_s3(records, bucket, parquet_key)

    return convert_to_parquet
