"""
Airflow failure callbacks for Comtrade DAGs.

On every task failure (after retries are exhausted) two things happen:

1. **Dead-letter manifest** — a structured JSON file is written to S3 under
   ``comtrade/errors/dag_id=.../task_id=.../year=.../month=.../``.
   This is a durable, Athena-queryable record of every failure regardless of
   whether Slack is configured.

2. **Slack notification** — a Block Kit message is posted to the configured
   Incoming Webhook.  If ``COMTRADE_SLACK_WEBHOOK_URL`` is not set the alert
   is suppressed with a warning log; the manifest is still written.

Design notes
------------
* Pure stdlib HTTP (``urllib.request``) — no extra dependencies.
* All Airflow / boto3 imports are lazy so this module can be imported and
  unit-tested without Airflow or AWS credentials.
* All side-effects (S3 write, Slack POST) are caught and logged; they must
  never mask the original task failure.
"""
from __future__ import annotations

import json
import logging
import urllib.request
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# Maximum characters of the exception message included in the Slack message.
_MAX_EXCEPTION_LEN = 400


# ── Internal helpers ──────────────────────────────────────────────────────────


def _get_webhook_url() -> Optional[str]:
    """Return the Slack webhook URL from Airflow Variables, or None if unset."""
    try:
        from airflow.models import Variable

        return Variable.get("COMTRADE_SLACK_WEBHOOK_URL", default_var=None)
    except Exception:
        return None


def _post_slack(payload: Dict[str, Any], webhook_url: str) -> None:
    """POST *payload* as JSON to *webhook_url*."""
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        webhook_url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:  # noqa: S310
        if resp.status != 200:
            body = resp.read().decode(errors="replace")
            logger.error("Slack responded HTTP %s: %s", resp.status, body)


def _build_task_failure_payload(context: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build a Slack Block Kit payload from an Airflow task context dict.

    Exposed as a standalone function so it can be tested without I/O.
    """
    dag_id = context["dag"].dag_id
    ti = context["task_instance"]
    task_id = ti.task_id
    run_id = context.get("run_id", "unknown")
    execution_date = str(context.get("execution_date", context.get("logical_date", "?")))
    log_url = ti.log_url
    exception = context.get("exception")
    exc_text = str(exception)[:_MAX_EXCEPTION_LEN] if exception else "No exception captured."

    return {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": ":red_circle: Comtrade Task Failed",
                    "emoji": True,
                },
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*DAG:*\n`{dag_id}`"},
                    {"type": "mrkdwn", "text": f"*Task:*\n`{task_id}`"},
                    {"type": "mrkdwn", "text": f"*Run ID:*\n`{run_id}`"},
                    {"type": "mrkdwn", "text": f"*Execution Date:*\n{execution_date}"},
                ],
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Error:*\n```{exc_text}```",
                },
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "View Task Logs", "emoji": True},
                        "url": log_url,
                        "style": "danger",
                    }
                ],
            },
        ]
    }


# ── Dead-letter manifest ──────────────────────────────────────────────────────


def _build_error_manifest(context: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build the error manifest dict from an Airflow task context.

    Pure function — no I/O.  Exposed for unit testing.
    """
    from datetime import datetime, timezone

    dag_id = context["dag"].dag_id
    ti = context["task_instance"]
    execution_date = context.get("execution_date", context.get("logical_date"))
    exception = context.get("exception")

    return {
        "schema_version": "1",
        "dag_id": dag_id,
        "task_id": ti.task_id,
        "run_id": context.get("run_id", "unknown"),
        "execution_date": str(execution_date) if execution_date is not None else None,
        "failed_at": datetime.now(timezone.utc).isoformat(),
        "exception_type": type(exception).__name__ if exception is not None else None,
        "exception_message": str(exception) if exception is not None else None,
    }


def write_error_manifest(context: Dict[str, Any]) -> Optional[str]:
    """
    Write a structured JSON error manifest to S3.

    S3 key pattern (Hive-partitioned for Athena):
        comtrade/errors/dag_id=<dag>/task_id=<task>/year=YYYY/month=MM/<run_id>.json

    Returns the S3 key on success, ``None`` on failure.  Never raises.
    """
    try:
        from datetime import datetime, timezone

        import boto3
        from airflow.models import Variable

        manifest = _build_error_manifest(context)

        execution_date = context.get("execution_date", context.get("logical_date"))
        if hasattr(execution_date, "strftime"):
            year = execution_date.strftime("%Y")
            month = execution_date.strftime("%m")
        else:
            _now = datetime.now(timezone.utc)
            year, month = _now.strftime("%Y"), _now.strftime("%m")

        safe_run_id = manifest["run_id"].replace(":", "-").replace("+", "-")
        key = (
            f"comtrade/errors"
            f"/dag_id={manifest['dag_id']}"
            f"/task_id={manifest['task_id']}"
            f"/year={year}/month={month}"
            f"/{safe_run_id}.json"
        )

        bucket = Variable.get("COMTRADE_S3_BUCKET")
        boto3.client(
            "s3",
            aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID", default_var=None),
            aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY", default_var=None),
            region_name=Variable.get("AWS_DEFAULT_REGION", default_var="us-east-1"),
        ).put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(manifest, default=str).encode(),
            ContentType="application/json",
        )

        logger.info("Error manifest written: s3://%s/%s", bucket, key)
        return key

    except Exception as exc:
        logger.error("Failed to write error manifest to S3: %s", exc)
        return None


# ── Public callbacks ──────────────────────────────────────────────────────────


def _build_sla_miss_payload(
    dag: Any,
    task_list: Any,
    blocking_task_list: Any,
    slas: Any,
    blocking_tis: Any,
) -> Dict[str, Any]:
    """
    Build a Slack Block Kit payload from Airflow SLA miss arguments.

    Exposed as a standalone function so it can be tested without I/O.
    The parameters mirror Airflow's ``sla_miss_callback`` signature exactly.
    """
    dag_id = dag.dag_id if hasattr(dag, "dag_id") else str(dag)

    missed = ", ".join(f"`{t}`" for t in (task_list or [])) or "unknown"
    blocking = ", ".join(f"`{t}`" for t in (blocking_task_list or [])) or "none"

    exec_dates: list[str] = []
    for sla in slas or []:
        if hasattr(sla, "execution_date"):
            exec_dates.append(str(sla.execution_date))
    exec_info = ", ".join(exec_dates) if exec_dates else "unknown"

    return {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": ":warning: Comtrade SLA Miss",
                    "emoji": True,
                },
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*DAG:*\n`{dag_id}`"},
                    {"type": "mrkdwn", "text": f"*Missed Tasks:*\n{missed}"},
                    {"type": "mrkdwn", "text": f"*Blocking Tasks:*\n{blocking}"},
                    {"type": "mrkdwn", "text": f"*Execution Date(s):*\n{exec_info}"},
                ],
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": ":clock1: One or more tasks did not complete within their SLA window.",
                },
            },
        ]
    }


def sla_miss_callback(
    dag: Any,
    task_list: Any,
    blocking_task_list: Any,
    slas: Any,
    blocking_tis: Any,
) -> None:
    """
    Airflow ``sla_miss_callback`` for DAGs.

    Add to the DAG definition:

        with DAG(..., sla_miss_callback=sla_miss_callback) as dag: ...

    And set a per-task SLA in ``default_args``:

        default_args = {"sla": timedelta(hours=8), ...}
    """
    webhook_url = _get_webhook_url()
    if not webhook_url:
        dag_id = dag.dag_id if hasattr(dag, "dag_id") else str(dag)
        logger.warning(
            "COMTRADE_SLACK_WEBHOOK_URL not configured — SLA miss alert suppressed for %s",
            dag_id,
        )
        return

    try:
        payload = _build_sla_miss_payload(dag, task_list, blocking_task_list, slas, blocking_tis)
        _post_slack(payload, webhook_url)
        logger.info("Slack SLA miss alert sent.")
    except Exception as exc:
        logger.error("Failed to send Slack SLA miss alert: %s", exc)


def task_failure_callback(context: Dict[str, Any]) -> None:
    """
    Airflow ``on_failure_callback`` for tasks.

    Pass this to the ``@task`` decorator or to ``default_args``:

        @task(on_failure_callback=task_failure_callback)
        def my_task(): ...

    Or at DAG level so all tasks inherit it:

        default_args = {"on_failure_callback": task_failure_callback}
    """
    # 1. Durable dead-letter record — always, regardless of Slack config.
    write_error_manifest(context)

    # 2. Real-time Slack notification — only when webhook is configured.
    webhook_url = _get_webhook_url()
    if not webhook_url:
        logger.warning(
            "COMTRADE_SLACK_WEBHOOK_URL not configured — Slack alert suppressed for %s.%s",
            context.get("dag", {}).dag_id if hasattr(context.get("dag", {}), "dag_id") else "?",
            context.get("task_instance", {}).task_id
            if hasattr(context.get("task_instance", {}), "task_id")
            else "?",
        )
        return

    try:
        payload = _build_task_failure_payload(context)
        _post_slack(payload, webhook_url)
        logger.info("Slack failure alert sent.")
    except Exception as exc:
        # Notification errors must never propagate — the task failure is what matters.
        logger.error("Failed to send Slack alert: %s", exc)
