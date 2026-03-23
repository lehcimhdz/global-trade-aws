"""
Schema drift detection for the Comtrade pipeline.

After every successful API extraction the current column set is compared
against the last known schema for that endpoint, which is stored as a small
JSON file in S3:

    s3://<COMTRADE_S3_BUCKET>/comtrade/schemas/<endpoint>.json

Behaviour
---------
* First run   — no previous schema found → current schema is saved, INFO logged.
* No change   — columns identical → nothing written, DEBUG logged.
* Drift found — added/removed columns logged as WARNING and a Slack Block Kit
                alert is sent.  The schema is then **updated** so that the
                next run compares against the new baseline.
* Any error   — caught and logged; drift detection must never fail the pipeline.

Design notes
------------
* Pure-stdlib HTTP (urllib.request) for Slack — no extra dependencies.
* All Airflow / boto3 imports are lazy for testability without either installed.
* ``_extract_columns``, ``_schema_s3_key``, ``_build_drift_payload``,
  and ``SchemaDriftResult`` are exposed as public symbols for unit testing.
"""
from __future__ import annotations

import json
import logging
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)

_SCHEMA_PREFIX = "comtrade/schemas"


# ── Data types ────────────────────────────────────────────────────────────────


@dataclass
class SchemaDriftResult:
    """Describes a detected change in the column set for an endpoint."""

    endpoint: str
    previous_columns: Set[str]
    current_columns: Set[str]
    added: Set[str] = field(default_factory=set)
    removed: Set[str] = field(default_factory=set)

    def __post_init__(self) -> None:
        if not self.added and not self.removed:
            self.added = self.current_columns - self.previous_columns
            self.removed = self.previous_columns - self.current_columns

    @property
    def has_drift(self) -> bool:
        return bool(self.added or self.removed)


# ── Pure helpers ──────────────────────────────────────────────────────────────


def _schema_s3_key(endpoint: str) -> str:
    """Return the S3 key used to store the schema for *endpoint*."""
    return f"{_SCHEMA_PREFIX}/{endpoint}.json"


def _extract_columns(records: List[Dict[str, Any]]) -> Set[str]:
    """
    Return the union of all keys across *records*.

    Using the union (not just the first record) is defensive against sparse
    responses where some columns appear only in certain records.
    """
    columns: Set[str] = set()
    for record in records:
        if isinstance(record, dict):
            columns.update(record.keys())
    return columns


def _build_drift_payload(result: SchemaDriftResult) -> Dict[str, Any]:
    """
    Build a Slack Block Kit payload describing the drift.

    Pure function — exposed for unit testing.
    """
    added_text = (
        "\n".join(f"  + `{c}`" for c in sorted(result.added))
        if result.added
        else "  _(none)_"
    )
    removed_text = (
        "\n".join(f"  - `{c}`" for c in sorted(result.removed))
        if result.removed
        else "  _(none)_"
    )

    return {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": ":warning: Comtrade Schema Drift Detected",
                    "emoji": True,
                },
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Endpoint:*\n`{result.endpoint}`"},
                    {
                        "type": "mrkdwn",
                        "text": (
                            f"*Change summary:*\n"
                            f"+{len(result.added)} added, -{len(result.removed)} removed"
                        ),
                    },
                ],
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Columns added:*\n{added_text}",
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Columns removed:*\n{removed_text}",
                },
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": (
                            "Schema has been updated to the new baseline. "
                            "Review downstream queries and dbt models."
                        ),
                    }
                ],
            },
        ]
    }


# ── AWS / Airflow helpers (lazy imports) ──────────────────────────────────────


def _get_webhook_url() -> Optional[str]:
    try:
        from airflow.models import Variable

        return Variable.get("COMTRADE_SLACK_WEBHOOK_URL", default_var=None)
    except Exception:
        return None


def _s3_client():
    import boto3
    from airflow.models import Variable

    return boto3.client(
        "s3",
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID", default_var=None),
        aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY", default_var=None),
        region_name=Variable.get("AWS_DEFAULT_REGION", default_var="us-east-1"),
    )


def _load_schema(bucket: str, endpoint: str) -> Optional[Set[str]]:
    """
    Load the stored schema for *endpoint* from S3.

    Returns the column set, or ``None`` if no schema has been saved yet.
    """
    try:
        key = _schema_s3_key(endpoint)
        obj = _s3_client().get_object(Bucket=bucket, Key=key)
        payload = json.loads(obj["Body"].read())
        return set(payload.get("columns", []))
    except Exception as exc:
        # NoSuchKey is expected on the first run; other errors are also non-fatal.
        if "NoSuchKey" in type(exc).__name__ or "404" in str(exc):
            return None
        logger.warning("Could not load schema for %s: %s", endpoint, exc)
        return None


def _save_schema(
    bucket: str,
    endpoint: str,
    columns: Set[str],
    run_id: str,
) -> None:
    """Persist the current column set to S3."""
    payload = {
        "schema_version": "1",
        "endpoint": endpoint,
        "columns": sorted(columns),
        "column_count": len(columns),
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id,
    }
    _s3_client().put_object(
        Bucket=bucket,
        Key=_schema_s3_key(endpoint),
        Body=json.dumps(payload, indent=2).encode(),
        ContentType="application/json",
    )


def _post_slack_alert(result: SchemaDriftResult) -> None:
    """Send a Slack alert for the detected drift."""
    url = _get_webhook_url()
    if not url:
        logger.warning(
            "COMTRADE_SLACK_WEBHOOK_URL not configured — schema drift alert suppressed for %s",
            result.endpoint,
        )
        return
    payload = _build_drift_payload(result)
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:  # noqa: S310
        if resp.status != 200:
            body = resp.read().decode(errors="replace")
            logger.error("Slack responded HTTP %s: %s", resp.status, body)


# ── Public entry point ────────────────────────────────────────────────────────


def detect_and_alert(
    bucket: str,
    endpoint: str,
    records: List[Dict[str, Any]],
    run_id: str = "unknown",
) -> Optional[SchemaDriftResult]:
    """
    Compare *records* column set against the stored schema for *endpoint*.

    Called automatically from ``make_validate_task`` after the quality gate
    passes.  All errors are caught — this must never fail the pipeline.

    Returns a ``SchemaDriftResult`` if drift was detected, ``None`` otherwise.
    """
    try:
        if not records:
            logger.debug("No records to inspect for schema drift on %s", endpoint)
            return None

        current = _extract_columns(records)
        previous = _load_schema(bucket, endpoint)

        if previous is None:
            # First time we've seen this endpoint — save baseline and move on.
            _save_schema(bucket, endpoint, current, run_id)
            logger.info(
                "Schema baseline captured for endpoint=%s columns=%d",
                endpoint,
                len(current),
            )
            return None

        result = SchemaDriftResult(
            endpoint=endpoint,
            previous_columns=previous,
            current_columns=current,
        )

        if not result.has_drift:
            logger.debug("No schema drift for endpoint=%s", endpoint)
            return None

        logger.warning(
            "Schema drift detected for endpoint=%s added=%s removed=%s",
            endpoint,
            sorted(result.added),
            sorted(result.removed),
        )

        try:
            _post_slack_alert(result)
        except Exception as exc:
            logger.error("Failed to send schema drift Slack alert: %s", exc)

        # Update baseline so next run compares against the new schema.
        _save_schema(bucket, endpoint, current, run_id)
        return result

    except Exception as exc:
        logger.error("Schema drift detection failed for %s: %s", endpoint, exc)
        return None
