"""
Apache Iceberg writer for the Comtrade pipeline.

Appends validated trade records to a Glue-backed Iceberg table, providing:

  ACID writes        — partial failures leave no corrupt state; the snapshot
                       is only committed when all files are written.
  Time travel        — query data ``AS OF`` any past snapshot or timestamp.
  Partition evolution — partitioning scheme can be changed without rewriting
                       historical data.
  Schema evolution   — new API columns are added automatically via
                       ``union_by_name``; no manual migration required.

Table layout
------------
  Glue database : ``comtrade``
  Table name    : ``<endpoint>``   e.g. ``preview``, ``getMBS``
  S3 location   : ``s3://<bucket>/iceberg/<endpoint>/``

All ``pyiceberg`` imports are lazy so this module can be imported and
unit-tested without the package installed.  Errors are caught and logged;
Iceberg failures must never fail the pipeline.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_GLUE_DATABASE = "comtrade"


# ── Pure helpers ──────────────────────────────────────────────────────────────


def _warehouse_uri(bucket: str) -> str:
    return f"s3://{bucket}/iceberg"


def _table_identifier(endpoint: str) -> str:
    return f"{_GLUE_DATABASE}.{endpoint}"


def _table_location(bucket: str, endpoint: str) -> str:
    return f"{_warehouse_uri(bucket)}/{endpoint}"


def _add_loaded_at(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Inject a ``_loaded_at`` UTC ISO-8601 timestamp into every record.

    dbt source freshness uses this column (``loaded_at_field: _loaded_at``) to
    determine how stale the bronze tables are.  All records in a single batch
    share the same timestamp so the column is monotonically non-decreasing across
    pipeline runs.
    """
    now = datetime.now(timezone.utc).isoformat()
    return [{**r, "_loaded_at": now} for r in records]


def _records_to_pa_table(records: List[Dict[str, Any]]):
    """Convert *records* to a PyArrow Table with inferred schema."""
    import pyarrow as pa  # available — in requirements.txt

    return pa.Table.from_pylist(records)


# ── Catalog / table helpers ───────────────────────────────────────────────────


def _get_catalog(region: str, warehouse: str):
    """Return a configured PyIceberg Glue catalog."""
    from pyiceberg.catalog import load_catalog

    return load_catalog(
        "glue",
        **{
            "type": "glue",
            "region_name": region,
            "warehouse": warehouse,
        },
    )


def _ensure_namespace(catalog, database: str) -> None:
    """Create the Glue database if it does not already exist."""
    try:
        existing = [ns[0] for ns in catalog.list_namespaces()]
        if database not in existing:
            catalog.create_namespace(database)
            logger.info("Created Iceberg namespace: %s", database)
    except Exception as exc:
        logger.warning("Could not create namespace %s: %s", database, exc)


def _load_or_create_table(catalog, identifier: str, pa_table, location: str):
    """
    Return the Iceberg table for *identifier*, creating it if necessary.

    If the table already exists and the current records contain new columns,
    the schema is evolved via ``union_by_name`` so downstream queries always
    see the latest column set.
    """
    from pyiceberg.exceptions import NoSuchTableError

    try:
        table = catalog.load_table(identifier)
        logger.debug("Loaded existing Iceberg table: %s", identifier)

        # Evolve schema if new columns are present.
        with table.update_schema() as update:
            update.union_by_name(pa_table.schema)

    except NoSuchTableError:
        db = identifier.split(".")[0]
        _ensure_namespace(catalog, db)
        table = catalog.create_table(
            identifier=identifier,
            schema=pa_table.schema,
            location=location,
        )
        logger.info("Created Iceberg table: %s at %s", identifier, location)

    return table


# ── Public API ────────────────────────────────────────────────────────────────


def write_to_iceberg(
    records: List[Dict[str, Any]],
    endpoint: str,
    bucket: str,
    region: str = "us-east-1",
) -> Optional[str]:
    """
    Append *records* to the Iceberg table for *endpoint*.

    Parameters
    ----------
    records:
        List of dicts from the Comtrade API ``data`` array.
    endpoint:
        Comtrade endpoint name — used as the Glue table name.
    bucket:
        S3 bucket that holds the data lake (``COMTRADE_S3_BUCKET``).
    region:
        AWS region for the Glue catalog and S3.

    Returns the Iceberg table identifier (``comtrade.<endpoint>``) on success,
    ``None`` on failure or when *records* is empty.
    """
    try:
        if not records:
            logger.warning("No records to write to Iceberg for endpoint=%s", endpoint)
            return None

        pa_table = _records_to_pa_table(_add_loaded_at(records))
        warehouse = _warehouse_uri(bucket)
        catalog = _get_catalog(region=region, warehouse=warehouse)

        identifier = _table_identifier(endpoint)
        location = _table_location(bucket, endpoint)

        table = _load_or_create_table(catalog, identifier, pa_table, location)
        table.append(pa_table)

        snapshot = table.current_snapshot()
        snapshot_id = snapshot.snapshot_id if snapshot else "?"
        logger.info(
            "Iceberg append committed — table=%s rows=%d snapshot=%s",
            identifier,
            len(pa_table),
            snapshot_id,
        )
        return identifier

    except Exception as exc:
        logger.error("Failed to write Iceberg table for %s: %s", endpoint, exc)
        return None
