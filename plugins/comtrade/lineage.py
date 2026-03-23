"""
OpenLineage event emission for the Comtrade pipeline.

Emits ``RunEvent`` JSON to a Marquez-compatible HTTP endpoint after each
task completes so that dataset-to-dataset dependencies are recorded:

  comtradeapi.un.org → [extract] → s3://bronze.json
  s3://bronze.json   → [validate] → (quality gate, no new dataset)
  s3://bronze.json   → [parquet]  → s3://silver.parquet

Events are posted to ``<OPENLINEAGE_URL>/api/v1/lineage`` using plain
``urllib.request`` — no extra Python dependency required.

Design notes
------------
* All Airflow imports are lazy; the module can be imported and unit-tested
  without an Airflow installation.
* All side-effects are caught and logged; they must never mask task failures.
* ``runId`` is derived deterministically from the Airflow ``run_id`` string
  via UUID5 so that repeated calls produce the same identifier.
"""
from __future__ import annotations

import json
import logging
import urllib.request
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# OpenLineage producer URI identifying this pipeline.
_PRODUCER = "https://github.com/global-trade-aws/comtrade-pipeline"

# Stable UUID namespace for deriving run UUIDs from Airflow run_id strings.
_UUID_NAMESPACE = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")  # uuid.NAMESPACE_URL


# ── Pure helpers ──────────────────────────────────────────────────────────────


def _run_uuid(airflow_run_id: str) -> str:
    """Return a deterministic UUID string derived from *airflow_run_id*."""
    return str(uuid.uuid5(_UUID_NAMESPACE, airflow_run_id))


def _split_uri(uri: str) -> Tuple[str, str]:
    """
    Split *uri* into an (namespace, name) pair per OpenLineage convention.

    - ``s3://bucket/key``         → ``("s3://bucket", "key")``
    - ``https://host/path``       → ``("https://host", "/path")``
    - Anything else               → ``(uri, "/")``
    """
    if uri.startswith("s3://"):
        without_scheme = uri[len("s3://"):]
        slash_pos = without_scheme.find("/")
        if slash_pos == -1:
            return f"s3://{without_scheme}", "/"
        return f"s3://{without_scheme[:slash_pos]}", without_scheme[slash_pos + 1:]

    if uri.startswith(("http://", "https://")):
        parsed = urlparse(uri)
        return f"{parsed.scheme}://{parsed.netloc}", parsed.path or "/"

    return uri, "/"


def _build_dataset(uri: str) -> Dict[str, str]:
    namespace, name = _split_uri(uri)
    return {"namespace": namespace, "name": name}


def _build_run_event(
    event_type: str,
    dag_id: str,
    task_id: str,
    run_id: str,
    input_uris: List[str],
    output_uris: List[str],
) -> Dict[str, Any]:
    """
    Build an OpenLineage ``RunEvent`` dict.

    Pure function — no I/O.  Exposed for unit testing.

    Parameters
    ----------
    event_type:  ``"START"``, ``"COMPLETE"``, or ``"FAIL"``.
    dag_id:      Airflow DAG id — used as part of the job name.
    task_id:     Airflow task id.
    run_id:      Airflow run_id string (will be hashed to a UUID).
    input_uris:  List of S3 or HTTP URIs consumed by this task.
    output_uris: List of S3 or HTTP URIs produced by this task.
    """
    return {
        "eventType": event_type,
        "eventTime": datetime.now(timezone.utc).isoformat(),
        "producer": _PRODUCER,
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
        "run": {"runId": _run_uuid(run_id)},
        "job": {
            "namespace": "comtrade",
            "name": f"{dag_id}.{task_id}",
        },
        "inputs": [_build_dataset(u) for u in input_uris],
        "outputs": [_build_dataset(u) for u in output_uris],
    }


# ── Transport ─────────────────────────────────────────────────────────────────


def _get_openlineage_url() -> Optional[str]:
    """Return the OpenLineage HTTP endpoint URL from Airflow Variables, or None."""
    try:
        from airflow.models import Variable

        return Variable.get("OPENLINEAGE_URL", default_var=None)
    except Exception:
        return None


def _post_event(event: Dict[str, Any], base_url: str) -> None:
    """POST *event* as JSON to ``<base_url>/api/v1/lineage``."""
    url = base_url.rstrip("/") + "/api/v1/lineage"
    data = json.dumps(event, default=str).encode()
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=5) as resp:  # noqa: S310
        if resp.status not in (200, 201, 202):
            body = resp.read().decode(errors="replace")
            logger.error("OpenLineage server responded HTTP %s: %s", resp.status, body)


# ── Public API ────────────────────────────────────────────────────────────────


def emit_task_complete(
    dag_id: str,
    task_id: str,
    run_id: str,
    input_uris: List[str],
    output_uris: List[str],
) -> None:
    """
    Emit an OpenLineage ``COMPLETE`` event for a finished task.

    Parameters
    ----------
    dag_id:       Airflow DAG id.
    task_id:      Airflow task id.
    run_id:       Airflow run_id string.
    input_uris:   Data sources consumed (S3 URIs or API URLs).
    output_uris:  Data sources produced (S3 URIs).

    If ``OPENLINEAGE_URL`` is not configured the call is silently skipped.
    Any error is logged without re-raising — lineage failures must never
    mask task failures.
    """
    try:
        url = _get_openlineage_url()
        if not url:
            logger.debug(
                "OPENLINEAGE_URL not configured — lineage event skipped for %s.%s",
                dag_id,
                task_id,
            )
            return

        event = _build_run_event(
            event_type="COMPLETE",
            dag_id=dag_id,
            task_id=task_id,
            run_id=run_id,
            input_uris=input_uris,
            output_uris=output_uris,
        )
        _post_event(event, url)
        logger.info(
            "OpenLineage COMPLETE emitted — job=comtrade.%s.%s inputs=%d outputs=%d",
            dag_id,
            task_id,
            len(input_uris),
            len(output_uris),
        )
    except Exception as exc:
        logger.error("Failed to emit OpenLineage event: %s", exc)
