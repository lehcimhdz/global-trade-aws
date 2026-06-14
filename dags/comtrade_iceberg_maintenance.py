"""
DAG: comtrade_iceberg_maintenance

Weekly housekeeping for the Comtrade Iceberg tables — runs Athena's
``OPTIMIZE`` (bin-pack small files) and ``VACUUM`` (expire snapshots and
remove orphan data files) so query latency and S3 storage cost stay flat as
the bronze and silver tables grow.

Iceberg snapshots older than the table's ``vacuum_min_snapshots_to_keep`` /
``vacuum_max_snapshot_age_seconds`` window are dropped along with their
data files; we let Iceberg's own retention metadata drive the cut-off rather
than passing a CLI flag so the policy stays declarative.

Maintained tables (all in Athena via the Glue Data Catalog):

    comtrade (bronze):     preview, previewTariffline, getMBS, getDA,
                           getDATariffline, getWorldShare, getMetadata,
                           getComtradeReleases
    comtrade_silver:       reporter_summary, trade_flows

Schedule:
    Sunday 04:00 UTC — between the Saturday-night ingestion peak and the
    Monday business-hour Athena/QuickSight traffic.

Airflow Variables used:
    ATHENA_WORKGROUP        — Athena workgroup           (default: comtrade)
    ATHENA_OUTPUT_LOCATION  — s3://<bucket>/athena-results/ (required)
    AWS_DEFAULT_REGION      — AWS region                  (default: us-east-1)
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from comtrade.callbacks import sla_miss_callback, task_failure_callback

logger = logging.getLogger(__name__)


BRONZE_DB = "comtrade"
SILVER_DB = "comtrade_silver"

BRONZE_TABLES = [
    "preview",
    "previewTariffline",
    "getMBS",
    "getDA",
    "getDATariffline",
    "getWorldShare",
    "getMetadata",
    "getComtradeReleases",
]

SILVER_TABLES = ["reporter_summary", "trade_flows"]


def _athena_client(region: str):
    """Lazy boto3 import so the DAG file parses without AWS deps installed."""
    import boto3

    return boto3.client("athena", region_name=region)


def _run_athena_statement(sql: str, workgroup: str, output_location: str, region: str) -> None:
    """Run a single DDL statement synchronously, polling until terminal."""
    client = _athena_client(region)
    execution = client.start_query_execution(
        QueryString=sql,
        WorkGroup=workgroup,
        ResultConfiguration={"OutputLocation": output_location},
    )
    qid: str = execution["QueryExecutionId"]

    for attempt in range(180):  # ~15 min upper bound (5s avg)
        status = client.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        state = status["State"]
        if state == "SUCCEEDED":
            return
        if state in ("FAILED", "CANCELLED"):
            reason = status.get("StateChangeReason", "Unknown")
            raise RuntimeError(f"Athena statement {state}: {reason}\nSQL: {sql}")
        time.sleep(min(2 + attempt * 0.1, 5.0))

    raise TimeoutError(f"Athena statement timed out: {sql}")


def _maintain_tables(database: str, tables: list[str], **context: object) -> None:
    """Run ``OPTIMIZE`` then ``VACUUM`` against each table in *tables*."""
    from airflow.models import Variable

    workgroup = Variable.get("ATHENA_WORKGROUP", default_var="comtrade")
    output_location = Variable.get("ATHENA_OUTPUT_LOCATION")
    region = Variable.get("AWS_DEFAULT_REGION", default_var="us-east-1")

    failures: list[str] = []
    for table in tables:
        fqtn = f'"{database}"."{table}"'
        for operation in ("OPTIMIZE", "VACUUM"):
            sql = (
                f"OPTIMIZE {fqtn} REWRITE DATA USING BIN_PACK"
                if operation == "OPTIMIZE"
                else f"VACUUM {fqtn}"
            )
            try:
                logger.info("Running %s on %s", operation, fqtn)
                _run_athena_statement(sql, workgroup, output_location, region)
            except Exception as exc:  # noqa: BLE001 — collect & report per-table
                logger.exception("%s failed on %s", operation, fqtn)
                failures.append(f"{operation} {fqtn}: {exc}")

    if failures:
        raise RuntimeError(
            f"Iceberg maintenance had {len(failures)} failure(s):\n" + "\n".join(failures)
        )


with DAG(
    dag_id="comtrade_iceberg_maintenance",
    description="Weekly OPTIMIZE + VACUUM of Comtrade Iceberg bronze and silver tables",
    schedule="0 4 * * 0",  # Sunday 04:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=15),
        "sla": timedelta(hours=4),
        "on_failure_callback": task_failure_callback,
    },
    sla_miss_callback=sla_miss_callback,
    tags=["comtrade", "iceberg", "maintenance"],
) as dag:

    maintain_bronze = PythonOperator(
        task_id="maintain_bronze",
        python_callable=_maintain_tables,
        op_kwargs={"database": BRONZE_DB, "tables": BRONZE_TABLES},
    )

    maintain_silver = PythonOperator(
        task_id="maintain_silver",
        python_callable=_maintain_tables,
        op_kwargs={"database": SILVER_DB, "tables": SILVER_TABLES},
    )

    maintain_bronze >> maintain_silver
