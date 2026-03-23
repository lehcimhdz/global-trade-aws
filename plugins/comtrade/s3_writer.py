"""
Helpers to write Comtrade API responses to S3 as JSON (raw) and optionally Parquet.

S3 key convention (Hive-compatible for Athena/Glue):
  comtrade/<endpoint>/type=<typeCode>/freq=<freqCode>/year=YYYY/month=MM/<run_id>.json
  comtrade/<endpoint>/type=<typeCode>/freq=<freqCode>/year=YYYY/month=MM/fmt=parquet/<run_id>.parquet

For endpoints without path params the type= and freq= partitions are omitted.
"""
from __future__ import annotations

import io
import json
import logging
from typing import Any, Dict, List, Optional

import boto3
from airflow.models import Variable

logger = logging.getLogger(__name__)


def _s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID", default_var=None),
        aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY", default_var=None),
        region_name=Variable.get("AWS_DEFAULT_REGION", default_var="us-east-1"),
    )


def build_s3_key(
    endpoint: str,
    run_id: str,
    year: str,
    month: str,
    typeCode: Optional[str] = None,
    freqCode: Optional[str] = None,
    extra_partitions: Optional[Dict[str, str]] = None,
    fmt: str = "json",
) -> str:
    parts = [f"comtrade/{endpoint}"]
    if typeCode:
        parts.append(f"type={typeCode}")
    if freqCode:
        parts.append(f"freq={freqCode}")
    if extra_partitions:
        for k, v in extra_partitions.items():
            parts.append(f"{k}={v}")
    parts.append(f"year={year}")
    parts.append(f"month={month}")
    if fmt == "parquet":
        parts.append("fmt=parquet")
        parts.append(f"{run_id}.parquet")
    else:
        parts.append(f"{run_id}.json")
    return "/".join(parts)


def write_json_to_s3(
    data: Any,
    bucket: str,
    key: str,
) -> str:
    """Serialize *data* as JSON and upload to S3. Returns the full s3:// URI."""
    body = json.dumps(data, ensure_ascii=False, default=str).encode("utf-8")
    client = _s3_client()
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")
    uri = f"s3://{bucket}/{key}"
    logger.info("Wrote JSON → %s (%d bytes)", uri, len(body))
    return uri


def write_parquet_to_s3(
    records: List[Dict],
    bucket: str,
    key: str,
) -> str:
    """Convert a list of dicts to Parquet and upload to S3. Returns the s3:// URI."""
    import pandas as pd  # lazy import — only needed when parquet is enabled

    df = pd.json_normalize(records)
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    client = _s3_client()
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.read(),
        ContentType="application/octet-stream",
    )
    uri = f"s3://{bucket}/{key}"
    logger.info("Wrote Parquet → %s", uri)
    return uri
