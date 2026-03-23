"""
Global Trade API — FastAPI application deployed as AWS Lambda via Mangum.

Endpoints
---------
GET /health
    Liveness probe.

GET /v1/reporters
    Top reporters ranked by total trade value.
    Query params: period (4-digit year), limit (1-200, default 20).

GET /v1/reporters/{reporter_iso}/summary
    Annual trade time-series for a single reporting country.
    Query params: limit (1-50, default 10).

GET /v1/trade-flows
    Commodity-level bilateral trade rows from the silver layer.
    Query params: reporter_iso, partner_iso, period, flow_code, limit (1-1000, default 100).

All data is sourced from the ``comtrade_silver`` Glue database via Athena.
Runtime configuration is supplied through environment variables set by Terraform.

Environment variables
---------------------
ATHENA_WORKGROUP       Athena workgroup name.
ATHENA_OUTPUT_LOCATION S3 URI for Athena result files (e.g. s3://bucket/athena-results/).
AWS_DEFAULT_REGION     AWS region (default: us-east-1).
"""
from __future__ import annotations

import os
import re
from typing import Annotated, Any, Optional

from fastapi import FastAPI, HTTPException, Path, Query
from fastapi.responses import JSONResponse

from api.athena import AthenaQueryError, run_query

# ── Runtime config ────────────────────────────────────────────────────────────

_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "")
_OUTPUT_LOCATION = os.environ.get("ATHENA_OUTPUT_LOCATION", "")
_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

# ── Input validation patterns ─────────────────────────────────────────────────

_ISO_PATTERN = r"^[A-Za-z]{2,3}$"
_PERIOD_PATTERN = r"^\d{4}$"
_FLOW_PATTERN = r"^[A-Za-z]{1,2}$"


def _iso(raw: str) -> str:
    """Normalise and validate a 2-3 letter ISO country code."""
    v = raw.strip().upper()
    if not re.match(_ISO_PATTERN, v):
        raise HTTPException(status_code=422, detail=f"Invalid ISO code: {raw!r}")
    return v


def _period(raw: str) -> str:
    """Validate a 4-digit year string."""
    if not re.match(_PERIOD_PATTERN, raw.strip()):
        raise HTTPException(status_code=422, detail=f"Invalid period: {raw!r} (expected YYYY)")
    return raw.strip()


# ── App ───────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Global Trade API",
    description="REST interface over the Comtrade silver Iceberg tables via Athena.",
    version="1.0.0",
)


# ── Routes ────────────────────────────────────────────────────────────────────


@app.get("/health", tags=["ops"])
def health() -> dict[str, str]:
    """Liveness probe — always returns 200."""
    return {"status": "ok"}


@app.get("/v1/reporters", tags=["data"])
def list_reporters(
    period: Annotated[
        Optional[str],
        Query(pattern=_PERIOD_PATTERN, description="Filter to a single year (e.g. 2022)"),
    ] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 20,
) -> dict[str, Any]:
    """List reporting countries ranked by total trade value.

    Filtered to annual data (``freq_code = 'A'``) only.
    """
    conditions = ["freq_code = 'A'"]
    if period:
        conditions.append(f"period = '{_period(period)}'")
    where = "WHERE " + " AND ".join(conditions)

    sql = f"""
        SELECT
            reporter_iso,
            reporter_name,
            period,
            export_value_usd,
            import_value_usd,
            total_trade_value_usd,
            trade_balance_usd,
            commodity_count,
            partner_count
        FROM comtrade_silver.reporter_summary
        {where}
        ORDER BY total_trade_value_usd DESC
        LIMIT {limit}
    """
    return _execute(sql)


@app.get("/v1/reporters/{reporter_iso}/summary", tags=["data"])
def reporter_summary(
    reporter_iso: Annotated[str, Path(pattern=_ISO_PATTERN)],
    limit: Annotated[int, Query(ge=1, le=50)] = 10,
) -> dict[str, Any]:
    """Annual trade time-series for a single reporting country.

    Returns rows ordered by period descending (most recent first).
    """
    iso = _iso(reporter_iso)
    sql = f"""
        SELECT
            period,
            export_value_usd,
            import_value_usd,
            total_trade_value_usd,
            trade_balance_usd,
            commodity_count,
            partner_count
        FROM comtrade_silver.reporter_summary
        WHERE freq_code = 'A'
          AND reporter_iso = '{iso}'
        ORDER BY period DESC
        LIMIT {limit}
    """
    rows = _execute(sql)
    if not rows["data"]:
        raise HTTPException(
            status_code=404, detail=f"No data found for reporter_iso={iso!r}"
        )
    return {"reporter_iso": iso, "data": rows["data"]}


@app.get("/v1/trade-flows", tags=["data"])
def trade_flows(
    reporter_iso: Annotated[
        Optional[str], Query(pattern=_ISO_PATTERN)
    ] = None,
    partner_iso: Annotated[
        Optional[str], Query(pattern=_ISO_PATTERN)
    ] = None,
    period: Annotated[
        Optional[str], Query(pattern=_PERIOD_PATTERN)
    ] = None,
    flow_code: Annotated[
        Optional[str], Query(pattern=_FLOW_PATTERN, description="X (export) or M (import)")
    ] = None,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
) -> dict[str, Any]:
    """Commodity-level bilateral trade flows from the silver layer.

    At least one filter (reporter_iso, partner_iso, or period) is recommended to
    keep response sizes manageable.
    """
    conditions = ["freq_code = 'A'"]
    if reporter_iso:
        conditions.append(f"reporter_iso = '{_iso(reporter_iso)}'")
    if partner_iso:
        conditions.append(f"partner_iso = '{_iso(partner_iso)}'")
    if period:
        conditions.append(f"period = '{_period(period)}'")
    if flow_code:
        fc = flow_code.strip().upper()
        conditions.append(f"flow_code = '{fc}'")

    where = "WHERE " + " AND ".join(conditions)
    sql = f"""
        SELECT
            period,
            reporter_iso,
            reporter_name,
            partner_iso,
            partner_name,
            commodity_code,
            commodity_name,
            flow_code,
            flow_name,
            trade_value_usd
        FROM comtrade_silver.trade_flows
        {where}
        ORDER BY trade_value_usd DESC
        LIMIT {limit}
    """
    return _execute(sql)


# ── Internal helpers ──────────────────────────────────────────────────────────


def _execute(sql: str) -> dict[str, Any]:
    """Run *sql* via Athena and wrap the result in a ``{"data": [...]}`` envelope."""
    try:
        rows = run_query(sql, _WORKGROUP, _OUTPUT_LOCATION, _REGION)
    except AthenaQueryError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {"data": rows}


# ── Lambda entry-point ────────────────────────────────────────────────────────
# Mangum is imported lazily so the module can be used without it installed
# (e.g. in unit tests that run the FastAPI app directly via TestClient).
try:
    from mangum import Mangum

    handler = Mangum(app, lifespan="off")
except ImportError:  # pragma: no cover
    handler = None  # type: ignore[assignment]
