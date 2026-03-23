"""
Airflow failure callbacks for Comtrade DAGs.

Sends a Slack notification whenever a task fails (after retries are exhausted).
The webhook URL is read at call time from Airflow Variable
``COMTRADE_SLACK_WEBHOOK_URL`` (backed by AWS Secrets Manager in production).

If the variable is not set the callback logs a warning and returns silently —
this keeps local development friction-free without requiring a Slack workspace.

Design notes
------------
* Pure stdlib HTTP (``urllib.request``) — no extra dependencies.
* All Airflow imports are lazy (inside functions) so this module can be
  imported and unit-tested without an Airflow installation.
* Notification failures are caught and logged; they must never mask the
  original task failure.
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


# ── Public callbacks ──────────────────────────────────────────────────────────


def task_failure_callback(context: Dict[str, Any]) -> None:
    """
    Airflow ``on_failure_callback`` for tasks.

    Pass this to the ``@task`` decorator or to ``default_args``:

        @task(on_failure_callback=task_failure_callback)
        def my_task(): ...

    Or at DAG level so all tasks inherit it:

        default_args = {"on_failure_callback": task_failure_callback}
    """
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
