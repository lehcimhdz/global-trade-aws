"""
DAG: comtrade_preview
Endpoint: GET /public/v1/preview/{typeCode}/{freqCode}/{clCode}

Airflow Variables used:
  COMTRADE_S3_BUCKET       — target S3 bucket name
  COMTRADE_TYPE_CODE       — e.g. "C" (commodities)
  COMTRADE_FREQ_CODE       — e.g. "A" (annual) or "M" (monthly)
  COMTRADE_CL_CODE         — classification code, e.g. "HS"
  COMTRADE_REPORTER_CODE   — reporter country ISO numeric code(s), comma-separated
  COMTRADE_PERIOD          — period string, e.g. "2023" or "202301"
  COMTRADE_PARTNER_CODE    — partner country code(s)
  COMTRADE_CMD_CODE        — commodity code(s)
  COMTRADE_FLOW_CODE       — trade flow code, e.g. "X" (export) / "M" (import)
  COMTRADE_WRITE_PARQUET   — "true" / "false"
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable

from comtrade import client
from comtrade.dag_factory import make_extract_task, make_parquet_task

with DAG(
    dag_id="comtrade_preview",
    description="Extract Comtrade preview data and store in S3",
    schedule="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["comtrade", "preview", "s3"],
) as dag:

    def _api_kwargs():
        return dict(
            typeCode=Variable.get("COMTRADE_TYPE_CODE", default_var="C"),
            freqCode=Variable.get("COMTRADE_FREQ_CODE", default_var="A"),
            clCode=Variable.get("COMTRADE_CL_CODE", default_var="HS"),
            reporterCode=Variable.get("COMTRADE_REPORTER_CODE", default_var=None),
            period=Variable.get("COMTRADE_PERIOD", default_var=None),
            partnerCode=Variable.get("COMTRADE_PARTNER_CODE", default_var=None),
            cmdCode=Variable.get("COMTRADE_CMD_CODE", default_var=None),
            flowCode=Variable.get("COMTRADE_FLOW_CODE", default_var=None),
        )

    typeCode = Variable.get("COMTRADE_TYPE_CODE", default_var="C")
    freqCode = Variable.get("COMTRADE_FREQ_CODE", default_var="A")

    extract = make_extract_task(
        endpoint="preview",
        api_fn=client.get_preview,
        api_kwargs_fn=_api_kwargs,
        typeCode=typeCode,
        freqCode=freqCode,
    )()

    to_parquet = make_parquet_task(
        endpoint="preview",
        typeCode=typeCode,
        freqCode=freqCode,
    )(json_key=extract)
