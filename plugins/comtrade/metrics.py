"""
CloudWatch custom metrics for the Comtrade pipeline.

Emitted after every ``validate_bronze`` task under the ``Comtrade/Pipeline``
namespace:

  RowCount         — number of records returned by the API
  ChecksPassed     — checks that passed (any severity)
  ChecksFailed     — checks that failed (any severity)
  JsonBytesWritten — size of the S3 bronze JSON object

Dimensions on every metric: DagId, Endpoint.

Design notes
------------
* All Airflow / boto3 imports are lazy — the module can be imported and
  unit-tested without either installed.
* All side-effects are caught and logged; they must never mask task failures.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

NAMESPACE = "Comtrade/Pipeline"


# ── Internal helpers ──────────────────────────────────────────────────────────


def _get_region() -> str:
    try:
        from airflow.models import Variable

        return Variable.get("AWS_DEFAULT_REGION", default_var="us-east-1")
    except Exception:
        return "us-east-1"


def _build_metric_data(
    dag_id: str,
    endpoint: str,
    results: List[Any],
    json_bytes: Optional[int],
) -> List[Dict[str, Any]]:
    """
    Build the MetricData list for put_metric_data.

    Pure function — exposed for unit testing.
    """
    dimensions = [
        {"Name": "DagId", "Value": dag_id},
        {"Name": "Endpoint", "Value": endpoint},
    ]

    passed = sum(1 for r in results if r.passed)
    failed = len(results) - passed

    row_count = 0
    for r in results:
        if r.name == "check_row_count" and r.details.get("row_count") is not None:
            row_count = int(r.details["row_count"])
            break

    data: List[Dict[str, Any]] = [
        {
            "MetricName": "RowCount",
            "Value": float(row_count),
            "Unit": "Count",
            "Dimensions": dimensions,
        },
        {
            "MetricName": "ChecksPassed",
            "Value": float(passed),
            "Unit": "Count",
            "Dimensions": dimensions,
        },
        {
            "MetricName": "ChecksFailed",
            "Value": float(failed),
            "Unit": "Count",
            "Dimensions": dimensions,
        },
    ]

    if json_bytes is not None:
        data.append(
            {
                "MetricName": "JsonBytesWritten",
                "Value": float(json_bytes),
                "Unit": "Bytes",
                "Dimensions": dimensions,
            }
        )

    return data


# ── Public API ─────────────────────────────────────────────────────────────────


def emit_validation_metrics(
    dag_id: str,
    endpoint: str,
    results: List[Any],
    json_bytes: Optional[int] = None,
) -> None:
    """
    Emit validation metrics to CloudWatch after a ``validate_bronze`` run.

    Parameters
    ----------
    dag_id:
        Airflow DAG id — used as the ``DagId`` CloudWatch dimension.
    endpoint:
        Comtrade endpoint name — used as the ``Endpoint`` dimension.
    results:
        List of ``CheckResult`` objects returned by ``run_checks()``.
    json_bytes:
        ContentLength of the S3 bronze JSON object in bytes.  Pass ``None``
        to skip the ``JsonBytesWritten`` metric.
    """
    try:
        import boto3

        metric_data = _build_metric_data(dag_id, endpoint, results, json_bytes)
        region = _get_region()

        boto3.client("cloudwatch", region_name=region).put_metric_data(
            Namespace=NAMESPACE,
            MetricData=metric_data,
        )

        row_count = next(
            (int(m["Value"]) for m in metric_data if m["MetricName"] == "RowCount"), 0
        )
        passed = next(
            (int(m["Value"]) for m in metric_data if m["MetricName"] == "ChecksPassed"), 0
        )
        failed = next(
            (int(m["Value"]) for m in metric_data if m["MetricName"] == "ChecksFailed"), 0
        )
        logger.info(
            "CloudWatch metrics emitted — dag=%s endpoint=%s rows=%d passed=%d failed=%d",
            dag_id,
            endpoint,
            row_count,
            passed,
            failed,
        )
    except Exception as exc:
        logger.error("Failed to emit CloudWatch metrics: %s", exc)
