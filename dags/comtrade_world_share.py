"""
DAG: comtrade_world_share
Endpoint: GET /public/v1/getWorldShare/{typeCode}/{freqCode}

Airflow Variables used:
  COMTRADE_S3_BUCKET
  COMTRADE_TYPE_CODE
  COMTRADE_FREQ_CODE
  COMTRADE_PERIOD
  COMTRADE_REPORTER_CODE
  COMTRADE_WRITE_PARQUET
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable

from comtrade import client
from comtrade.callbacks import sla_miss_callback
from comtrade.dag_factory import make_extract_task, make_parquet_task, make_validate_task

with DAG(
    dag_id="comtrade_world_share",
    description="Extract Comtrade world share data and store in S3",
    schedule="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5), "sla": timedelta(hours=8)},
    sla_miss_callback=sla_miss_callback,
    tags=["comtrade", "world-share", "s3"],
) as dag:

    def _api_kwargs():
        return dict(
            typeCode=Variable.get("COMTRADE_TYPE_CODE", default_var="C"),
            freqCode=Variable.get("COMTRADE_FREQ_CODE", default_var="A"),
            period=Variable.get("COMTRADE_PERIOD", default_var=None),
            reporterCode=Variable.get("COMTRADE_REPORTER_CODE", default_var=None),
        )

    typeCode = Variable.get("COMTRADE_TYPE_CODE", default_var="C")
    freqCode = Variable.get("COMTRADE_FREQ_CODE", default_var="A")

    extract = make_extract_task(
        endpoint="getWorldShare",
        api_fn=client.get_world_share,
        api_kwargs_fn=_api_kwargs,
        typeCode=typeCode,
        freqCode=freqCode,
    )()

    validated = make_validate_task(
        endpoint="getWorldShare",
        required_columns=["reporterCode", "period"],
        numeric_columns=["share"],
        dedup_columns=["reporterCode", "period"],
    )(json_key=extract)

    to_parquet = make_parquet_task(
        endpoint="getWorldShare",
        typeCode=typeCode,
        freqCode=freqCode,
    )(json_key=validated)
