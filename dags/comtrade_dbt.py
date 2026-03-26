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

The three model/test tasks (``dbt_run_staging``, ``dbt_run_silver``,
``dbt_test``) are implemented as PythonOperators that run dbt with
``--log-format json``, parse the structured output for error/failure counts,
and emit three CloudWatch metrics under the ``Comtrade/Pipeline`` namespace:

  DbtRunDuration    — wall-clock seconds for each dbt phase
  DbtModelsErrored  — count of models that finished with status ``error``
  DbtTestsFailed    — count of tests that finished with status ``fail``

Metrics are always emitted — even when the dbt command fails — so the
dashboard reflects partial runs accurately.  The task still raises on a
non-zero exit code so Airflow marks it as failed.

Airflow Variables used:
  COMTRADE_S3_BUCKET       — S3 bucket (forwarded to dbt as env var)
  AWS_DEFAULT_REGION       — AWS region                (default: us-east-1)
  AWS_ACCESS_KEY_ID        — AWS credentials           (omit if using IAM role)
  AWS_SECRET_ACCESS_KEY    — AWS credentials           (omit if using IAM role)
  COMTRADE_DBT_DIR         — path to the dbt project   (default: /opt/airflow/dbt)
  COMTRADE_DBT_TARGET      — dbt target profile        (default: prod)
"""
from __future__ import annotations

import json
import os
import subprocess
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from comtrade.callbacks import sla_miss_callback, task_failure_callback
from comtrade.metrics import emit_dbt_metrics

# ── Jinja-templated environment passed to BashOperators ───────────────────────
# Values are evaluated at runtime (task execution time), not at parse time, so
# Variable.get() is never called during DAG parsing.

_DBT_ENV = {
    "COMTRADE_S3_BUCKET":    "{{ var.value.COMTRADE_S3_BUCKET }}",
    "AWS_DEFAULT_REGION":    "{{ var.value.get('AWS_DEFAULT_REGION', 'us-east-1') }}",
    "AWS_ACCESS_KEY_ID":     "{{ var.value.get('AWS_ACCESS_KEY_ID', '') }}",
    "AWS_SECRET_ACCESS_KEY": "{{ var.value.get('AWS_SECRET_ACCESS_KEY', '') }}",
}

# ── Bash command builder (used for deps and freshness only) ───────────────────

_DBT_DIR    = "{{ var.value.get('COMTRADE_DBT_DIR', '/opt/airflow/dbt') }}"
_DBT_TARGET = "{{ var.value.get('COMTRADE_DBT_TARGET', 'prod') }}"


def _dbt(subcommand: str) -> str:
    """Return a bash command that cd's into the dbt project and runs *subcommand*."""
    return f"cd {_DBT_DIR} && dbt {subcommand} --target {_DBT_TARGET}"


# ── PythonOperator callable for instrumented dbt tasks ───────────────────────


def _run_dbt_and_emit(subcommand: str, phase: str, **context: object) -> None:
    """
    Run a dbt subcommand, parse its JSON log output, and emit CloudWatch metrics.

    Always emits ``DbtRunDuration``, ``DbtModelsErrored``, and ``DbtTestsFailed``
    before raising on a non-zero exit code so the dashboard reflects partial runs.

    Parameters
    ----------
    subcommand:
        dbt command to run, e.g. ``"run --select silver"``.
    phase:
        Task id string used as the ``Phase`` CloudWatch dimension.
    """
    from airflow.models import Variable  # lazy import — safe at parse time

    dbt_dir = Variable.get("COMTRADE_DBT_DIR", default_var="/opt/airflow/dbt")
    dbt_target = Variable.get("COMTRADE_DBT_TARGET", default_var="prod")

    env = {
        **os.environ,
        "COMTRADE_S3_BUCKET": Variable.get("COMTRADE_S3_BUCKET", default_var=""),
        "AWS_DEFAULT_REGION": Variable.get("AWS_DEFAULT_REGION", default_var="us-east-1"),
        "AWS_ACCESS_KEY_ID": Variable.get("AWS_ACCESS_KEY_ID", default_var=""),
        "AWS_SECRET_ACCESS_KEY": Variable.get("AWS_SECRET_ACCESS_KEY", default_var=""),
    }

    cmd = ["dbt"] + subcommand.split() + ["--target", dbt_target, "--log-format", "json"]

    t0 = time.monotonic()
    proc = subprocess.run(cmd, capture_output=True, text=True, cwd=dbt_dir, env=env)
    duration = time.monotonic() - t0

    # Parse structured JSON log lines for error/failure node statuses.
    models_errored = 0
    tests_failed = 0
    for line in proc.stdout.splitlines():
        try:
            entry = json.loads(line)
            status = (
                entry.get("data", {})
                .get("node_info", {})
                .get("node_status", "")
            )
            if status == "error":
                models_errored += 1
            elif status == "fail":
                tests_failed += 1
        except (json.JSONDecodeError, AttributeError, TypeError):
            continue

    # Always emit metrics — even on failure — before potentially raising.
    emit_dbt_metrics("comtrade_dbt", phase, duration, models_errored, tests_failed)

    if proc.returncode != 0:
        output = proc.stdout or proc.stderr
        # Truncate to last 10 000 chars to stay within Airflow log limits.
        print(output[-10_000:] if len(output) > 10_000 else output)
        raise RuntimeError(
            f"dbt {subcommand!r} exited with code {proc.returncode}"
        )


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

    dbt_run_staging = PythonOperator(
        task_id="dbt_run_staging",
        python_callable=_run_dbt_and_emit,
        op_kwargs={"subcommand": "run --select staging", "phase": "dbt_run_staging"},
    )

    dbt_run_silver = PythonOperator(
        task_id="dbt_run_silver",
        python_callable=_run_dbt_and_emit,
        op_kwargs={"subcommand": "run --select silver", "phase": "dbt_run_silver"},
    )

    dbt_test = PythonOperator(
        task_id="dbt_test",
        python_callable=_run_dbt_and_emit,
        op_kwargs={"subcommand": "test", "phase": "dbt_test"},
    )

    dbt_deps >> dbt_source_freshness >> dbt_run_staging >> dbt_run_silver >> dbt_test
