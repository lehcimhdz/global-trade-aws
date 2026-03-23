"""
DAG: comtrade_dbt
Runs the dbt silver layer transformations after the monthly ingestion window.

Reads from the Iceberg bronze tables registered in Glue (``comtrade`` database)
and materialises two silver Iceberg tables via Amazon Athena:
  - ``trade_flows``       — bilateral commodity-level aggregations
  - ``reporter_summary``  — per-country export / import / balance totals

Task order:
  dbt_deps → dbt_source_freshness → dbt_run_staging → dbt_run_silver → dbt_test

``dbt_source_freshness`` checks that each bronze Iceberg table was refreshed
within its expected window (warn: 26 days, error: 35 days).  If the check
errors the downstream model runs are skipped, preventing stale data from
propagating to the silver layer.

Airflow Variables used:
  COMTRADE_S3_BUCKET       — S3 bucket (forwarded to dbt as env var)
  AWS_DEFAULT_REGION       — AWS region                (default: us-east-1)
  AWS_ACCESS_KEY_ID        — AWS credentials           (omit if using IAM role)
  AWS_SECRET_ACCESS_KEY    — AWS credentials           (omit if using IAM role)
  COMTRADE_DBT_DIR         — path to the dbt project   (default: /opt/airflow/dbt)
  COMTRADE_DBT_TARGET      — dbt target profile        (default: prod)
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from comtrade.callbacks import sla_miss_callback, task_failure_callback

# ── Jinja-templated environment passed to every BashOperator ─────────────────
# Values are evaluated at runtime (task execution time), not at parse time, so
# Variable.get() is never called during DAG parsing.

_DBT_ENV = {
    "COMTRADE_S3_BUCKET":    "{{ var.value.COMTRADE_S3_BUCKET }}",
    "AWS_DEFAULT_REGION":    "{{ var.value.get('AWS_DEFAULT_REGION', 'us-east-1') }}",
    "AWS_ACCESS_KEY_ID":     "{{ var.value.get('AWS_ACCESS_KEY_ID', '') }}",
    "AWS_SECRET_ACCESS_KEY": "{{ var.value.get('AWS_SECRET_ACCESS_KEY', '') }}",
}

# ── Bash command builder ──────────────────────────────────────────────────────

_DBT_DIR    = "{{ var.value.get('COMTRADE_DBT_DIR', '/opt/airflow/dbt') }}"
_DBT_TARGET = "{{ var.value.get('COMTRADE_DBT_TARGET', 'prod') }}"


def _dbt(subcommand: str) -> str:
    """Return a bash command that cd's into the dbt project and runs *subcommand*."""
    return f"cd {_DBT_DIR} && dbt {subcommand} --target {_DBT_TARGET}"


# ── DAG ───────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="comtrade_dbt",
    description="Run dbt silver layer transformations on top of Comtrade Iceberg tables",
    schedule="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
        "sla": timedelta(hours=2),
        "on_failure_callback": task_failure_callback,
    },
    sla_miss_callback=sla_miss_callback,
    tags=["comtrade", "dbt", "silver"],
) as dag:

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {_DBT_DIR} && dbt deps",
        env=_DBT_ENV,
    )

    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=_dbt("source freshness"),
        env=_DBT_ENV,
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=_dbt("run --select staging"),
        env=_DBT_ENV,
    )

    dbt_run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=_dbt("run --select silver"),
        env=_DBT_ENV,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=_dbt("test"),
        env=_DBT_ENV,
    )

    dbt_deps >> dbt_source_freshness >> dbt_run_staging >> dbt_run_silver >> dbt_test
