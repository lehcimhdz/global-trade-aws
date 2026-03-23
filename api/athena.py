"""
Athena query runner for the trade API.

Starts a query execution, polls until complete, and returns the result rows as
a list of dicts keyed by column name.  The first page of the result set
contains the column header row which is stripped before returning.
"""
from __future__ import annotations

import time
from typing import Any

import boto3


class AthenaQueryError(Exception):
    """Raised when an Athena query fails or times out."""


def run_query(
    sql: str,
    workgroup: str,
    output_location: str,
    region: str,
    *,
    max_polls: int = 60,
) -> list[dict[str, Any]]:
    """Execute *sql* synchronously and return rows as a list of dicts.

    Args:
        sql: SQL statement to execute.
        workgroup: Athena workgroup name.
        output_location: S3 URI for query results (e.g. ``s3://bucket/prefix/``).
        region: AWS region string.
        max_polls: Maximum number of status polls before raising a timeout error.

    Returns:
        List of row dicts keyed by column name.  Empty list for zero-row results.

    Raises:
        AthenaQueryError: Query failed, was cancelled, or timed out.
    """
    client = boto3.client("athena", region_name=region)

    response = client.start_query_execution(
        QueryString=sql,
        WorkGroup=workgroup,
        ResultConfiguration={"OutputLocation": output_location},
    )
    execution_id: str = response["QueryExecutionId"]

    # Poll for completion with capped exponential back-off.
    for attempt in range(max_polls):
        status_resp = client.get_query_execution(QueryExecutionId=execution_id)
        state: str = status_resp["QueryExecution"]["Status"]["State"]

        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            reason = status_resp["QueryExecution"]["Status"].get(
                "StateChangeReason", "Unknown error"
            )
            raise AthenaQueryError(f"Athena query {state}: {reason}")

        time.sleep(min(0.25 * (2**attempt), 5.0))
    else:
        raise AthenaQueryError(
            f"Athena query timed out after {max_polls} polls (execution_id={execution_id})"
        )

    # Collect all result pages.
    rows: list[dict[str, Any]] = []
    columns: list[str] | None = None
    paginator = client.get_paginator("get_query_results")

    for page in paginator.paginate(QueryExecutionId=execution_id):
        page_rows = page["ResultSet"]["Rows"]
        if columns is None:
            # First row of the first page is always the column-header row.
            columns = [col.get("VarCharValue", "") for col in page_rows[0]["Data"]]
            page_rows = page_rows[1:]
        for raw_row in page_rows:
            values = [cell.get("VarCharValue") for cell in raw_row["Data"]]
            rows.append(dict(zip(columns, values)))

    return rows
