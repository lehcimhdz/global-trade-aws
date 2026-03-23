"""
DAG: comtrade_mbs
Endpoint: GET /public/v1/getMBS

MBS tables (T35 Total trade value since 1946, T38 Conversion factors).
No path params — partitions by series_type in S3 key.

Additional Airflow Variables:
  COMTRADE_MBS_SERIES_TYPE  — e.g. "T35" or "T38"
  COMTRADE_MBS_YEAR         — specific year filter
  COMTRADE_MBS_COUNTRY_CODE
  COMTRADE_MBS_PERIOD
  COMTRADE_MBS_PERIOD_TYPE
  COMTRADE_MBS_TABLE_TYPE
  COMTRADE_MBS_FORMAT
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable

from comtrade import client
from comtrade.dag_factory import make_extract_task, make_parquet_task

with DAG(
    dag_id="comtrade_mbs",
    description="Extract Comtrade MBS historical time-series and store in S3",
    schedule="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["comtrade", "mbs", "s3"],
) as dag:

    series_type = Variable.get("COMTRADE_MBS_SERIES_TYPE", default_var="T35")

    def _api_kwargs():
        return dict(
            series_type=Variable.get("COMTRADE_MBS_SERIES_TYPE", default_var="T35"),
            year=Variable.get("COMTRADE_MBS_YEAR", default_var=None),
            country_code=Variable.get("COMTRADE_MBS_COUNTRY_CODE", default_var=None),
            period=Variable.get("COMTRADE_MBS_PERIOD", default_var=None),
            period_type=Variable.get("COMTRADE_MBS_PERIOD_TYPE", default_var=None),
            table_type=Variable.get("COMTRADE_MBS_TABLE_TYPE", default_var=None),
            fmt=Variable.get("COMTRADE_MBS_FORMAT", default_var=None),
        )

    extract = make_extract_task(
        endpoint="getMBS",
        api_fn=client.get_mbs,
        api_kwargs_fn=_api_kwargs,
        extra_partitions={"series_type": series_type},
    )()

    to_parquet = make_parquet_task(
        endpoint="getMBS",
        extra_partitions={"series_type": series_type},
    )(json_key=extract)
