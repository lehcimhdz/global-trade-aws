"""
DAG: comtrade_preview_tariffline
Endpoint: GET /public/v1/previewTariffline/{typeCode}/{freqCode}/{clCode}

Additional Airflow Variables (beyond the base set in comtrade_preview):
  COMTRADE_PARTNER2_CODE  — second partner code
  COMTRADE_CUSTOMS_CODE   — customs procedure code
  COMTRADE_MOT_CODE       — mode of transport code
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable

from comtrade import client
from comtrade.callbacks import sla_miss_callback
from comtrade.dag_factory import make_extract_task, make_parquet_task, make_validate_task

with DAG(
    dag_id="comtrade_preview_tariffline",
    description="Extract Comtrade tariff-line preview data and store in S3",
    schedule="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5), "sla": timedelta(hours=8)},
    sla_miss_callback=sla_miss_callback,
    tags=["comtrade", "tariffline", "s3"],
) as dag:

    def _api_kwargs():
        return dict(
            typeCode=Variable.get("COMTRADE_TYPE_CODE", default_var="C"),
            freqCode=Variable.get("COMTRADE_FREQ_CODE", default_var="A"),
            clCode=Variable.get("COMTRADE_CL_CODE", default_var="HS"),
            reporterCode=Variable.get("COMTRADE_REPORTER_CODE", default_var=None),
            period=Variable.get("COMTRADE_PERIOD", default_var=None),
            partnerCode=Variable.get("COMTRADE_PARTNER_CODE", default_var=None),
            partner2Code=Variable.get("COMTRADE_PARTNER2_CODE", default_var=None),
            cmdCode=Variable.get("COMTRADE_CMD_CODE", default_var=None),
            flowCode=Variable.get("COMTRADE_FLOW_CODE", default_var=None),
            customsCode=Variable.get("COMTRADE_CUSTOMS_CODE", default_var=None),
            motCode=Variable.get("COMTRADE_MOT_CODE", default_var=None),
        )

    typeCode = Variable.get("COMTRADE_TYPE_CODE", default_var="C")
    freqCode = Variable.get("COMTRADE_FREQ_CODE", default_var="A")

    extract = make_extract_task(
        endpoint="previewTariffline",
        api_fn=client.get_preview_tariffline,
        api_kwargs_fn=_api_kwargs,
        typeCode=typeCode,
        freqCode=freqCode,
    )()

    validated = make_validate_task(
        endpoint="previewTariffline",
        required_columns=["reporterCode", "period"],
        numeric_columns=["primaryValue"],
        dedup_columns=["reporterCode", "partnerCode", "cmdCode", "flowCode", "period", "motCode"],
    )(json_key=extract)

    to_parquet = make_parquet_task(
        endpoint="previewTariffline",
        typeCode=typeCode,
        freqCode=freqCode,
    )(json_key=validated)
