"""
DAG: comtrade_backfill

Manually-triggered DAG for loading historical Comtrade data for one endpoint
across a list of periods.  All standard ingestion DAGs have ``catchup=False``
so past data is not automatically loaded; use this DAG to fill the gap.

Trigger via the Airflow UI (trigger w/ config) or the CLI:

    airflow dags trigger comtrade_backfill \\
      --conf '{
        "endpoint":      "preview",
        "periods":       ["2020", "2021", "2022"],
        "type_code":     "C",
        "freq_code":     "A",
        "cl_code":       "HS",
        "reporter_code": "842"
      }'

Or use the convenience script:

    ./scripts/trigger_backfill.sh \\
        --endpoint preview \\
        --periods  2020,2021,2022 \\
        --reporter 842

dag_run.conf keys
-----------------
endpoint       (required) — "preview", "previewTariffline", or "getMBS"
periods        (required) — list of period strings, e.g. ["2020", "2021"]
type_code      — default "C"
freq_code      — default "A"
cl_code        — default "HS"
reporter_code  — default None (all reporters)
partner_code   — default None
cmd_code       — default None
flow_code      — default None

Task order
----------
validate_conf → run_backfill

``validate_conf`` performs a fail-fast check before any API calls are made.
``run_backfill`` iterates over every period, calls the API, writes raw JSON
to S3, and returns a summary dict keyed by period.  If any period fails the
task is marked failed and the Slack callback fires.
"""
from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any, Dict, List, Optional

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.dates import days_ago

from comtrade import client
from comtrade.callbacks import task_failure_callback
from comtrade.s3_writer import build_s3_key, write_json_to_s3

logger = logging.getLogger(__name__)

# ── Supported endpoints ───────────────────────────────────────────────────────

SUPPORTED_ENDPOINTS: List[str] = ["preview", "previewTariffline", "getMBS"]


def _call_api(conf: Dict[str, Any], period: str) -> Dict:
    """Dispatch to the correct API client function based on ``conf["endpoint"]``."""
    endpoint = conf["endpoint"]
    type_code = conf.get("type_code", "C")
    freq_code = conf.get("freq_code", "A")
    cl_code = conf.get("cl_code", "HS")
    reporter_code = conf.get("reporter_code")
    partner_code = conf.get("partner_code")
    cmd_code = conf.get("cmd_code")
    flow_code = conf.get("flow_code")

    if endpoint == "preview":
        return client.get_preview(
            typeCode=type_code,
            freqCode=freq_code,
            clCode=cl_code,
            reporterCode=reporter_code,
            period=period,
            partnerCode=partner_code,
            cmdCode=cmd_code,
            flowCode=flow_code,
        )

    if endpoint == "previewTariffline":
        return client.get_preview_tariffline(
            typeCode=type_code,
            freqCode=freq_code,
            clCode=cl_code,
            reporterCode=reporter_code,
            period=period,
            partnerCode=partner_code,
            cmdCode=cmd_code,
            flowCode=flow_code,
        )

    if endpoint == "getMBS":
        return client.get_mbs(period=period)

    raise ValueError(
        f"Unsupported endpoint {endpoint!r}. "
        f"Choose from: {SUPPORTED_ENDPOINTS}"
    )


# ── DAG ───────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="comtrade_backfill",
    description=(
        "Manually-triggered historical backfill — "
        "loads one Comtrade endpoint for a list of periods"
    ),
    schedule=None,
    start_date=days_ago(1),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": task_failure_callback,
    },
    tags=["comtrade", "backfill"],
    render_template_as_native_obj=True,
) as dag:

    @task(task_id="validate_conf", on_failure_callback=task_failure_callback)
    def validate_conf(**context) -> Dict[str, Any]:
        """Validate dag_run.conf and return normalised parameters.

        Raises ``ValueError`` immediately if required keys are absent or
        the endpoint is not supported — before any API calls are made.
        """
        conf: Dict[str, Any] = context["dag_run"].conf or {}

        endpoint = conf.get("endpoint")
        if not endpoint:
            raise ValueError(
                "dag_run.conf is missing required key 'endpoint'. "
                f"Supported values: {SUPPORTED_ENDPOINTS}"
            )

        periods = conf.get("periods")
        if not periods or not isinstance(periods, list) or len(periods) == 0:
            raise ValueError(
                "dag_run.conf must include 'periods' as a non-empty list, "
                "e.g. [\"2020\", \"2021\"]"
            )

        if endpoint not in SUPPORTED_ENDPOINTS:
            raise ValueError(
                f"Unsupported endpoint {endpoint!r}. "
                f"Choose from: {SUPPORTED_ENDPOINTS}"
            )

        validated = {
            "endpoint":      endpoint,
            "periods":       periods,
            "type_code":     conf.get("type_code", "C"),
            "freq_code":     conf.get("freq_code", "A"),
            "cl_code":       conf.get("cl_code", "HS"),
            "reporter_code": conf.get("reporter_code"),
            "partner_code":  conf.get("partner_code"),
            "cmd_code":      conf.get("cmd_code"),
            "flow_code":     conf.get("flow_code"),
        }
        logger.info(
            "Backfill validated: endpoint=%s periods=%s",
            endpoint,
            periods,
        )
        return validated

    @task(task_id="run_backfill", on_failure_callback=task_failure_callback)
    def run_backfill(params: Dict[str, Any], **context) -> Dict[str, Any]:
        """Fetch and store raw JSON for every period in *params*.

        Returns a summary dict:  ``{period: {"status": "success"|"error",
        "key": ..., "error": ...}}``.  Raises ``RuntimeError`` at the end if
        any period failed so Airflow marks the task as failed.
        """
        endpoint = params["endpoint"]
        periods: List[str] = params["periods"]
        bucket = Variable.get("COMTRADE_S3_BUCKET")
        run_id = context["run_id"].replace(":", "-").replace("+", "-")

        results: Dict[str, Any] = {}

        for period in periods:
            try:
                data = _call_api(params, period)

                # Derive year/month from period string (YYYY or YYYYMM).
                year = period[:4]
                month = period[4:6] if len(period) >= 6 else "01"

                key = build_s3_key(
                    endpoint=endpoint,
                    run_id=run_id,
                    year=year,
                    month=month,
                    typeCode=params.get("type_code"),
                    freqCode=params.get("freq_code"),
                    fmt="json",
                )
                write_json_to_s3(data, bucket, key)
                results[period] = {"status": "success", "key": key}
                logger.info("Backfill success: endpoint=%s period=%s key=%s", endpoint, period, key)

            except Exception as exc:
                logger.error("Backfill error: endpoint=%s period=%s error=%s", endpoint, period, exc)
                results[period] = {"status": "error", "error": str(exc)}

        failed = [p for p, r in results.items() if r["status"] == "error"]
        if failed:
            raise RuntimeError(
                f"Backfill completed with errors for {len(failed)} period(s): {failed}. "
                "Check the task logs for details."
            )

        logger.info("Backfill complete: %d periods loaded successfully.", len(results))
        return results

    validated = validate_conf()
    run_backfill(params=validated)
