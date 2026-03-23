"""
DAG: comtrade_da
Endpoint: GET /public/v1/getDA/{typeCode}/{freqCode}/{clCode}
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable

from comtrade import client
from comtrade.dag_factory import make_extract_task, make_parquet_task

with DAG(
    dag_id="comtrade_da",
    description="Extract Comtrade DA data and store in S3",
    schedule="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["comtrade", "da", "s3"],
) as dag:

    def _api_kwargs():
        return dict(
            typeCode=Variable.get("COMTRADE_TYPE_CODE", default_var="C"),
            freqCode=Variable.get("COMTRADE_FREQ_CODE", default_var="A"),
            clCode=Variable.get("COMTRADE_CL_CODE", default_var="HS"),
        )

    typeCode = Variable.get("COMTRADE_TYPE_CODE", default_var="C")
    freqCode = Variable.get("COMTRADE_FREQ_CODE", default_var="A")

    extract = make_extract_task(
        endpoint="getDA",
        api_fn=client.get_da,
        api_kwargs_fn=_api_kwargs,
        typeCode=typeCode,
        freqCode=freqCode,
    )()

    to_parquet = make_parquet_task(
        endpoint="getDA",
        typeCode=typeCode,
        freqCode=freqCode,
    )(json_key=extract)
