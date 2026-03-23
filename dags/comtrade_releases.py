"""
DAG: comtrade_releases
Endpoint: GET /public/v1/getComtradeReleases

No path or query parameters. Runs daily to detect new releases.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG

from comtrade import client
from comtrade.dag_factory import make_extract_task, make_parquet_task, make_validate_task

with DAG(
    dag_id="comtrade_releases",
    description="Extract Comtrade release schedule and store in S3",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["comtrade", "releases", "s3"],
) as dag:

    extract = make_extract_task(
        endpoint="getComtradeReleases",
        api_fn=client.get_comtrade_releases,
        api_kwargs_fn=lambda: {},
    )()

    validated = make_validate_task(
        endpoint="getComtradeReleases",
        freq_code_variable=None,
    )(json_key=extract)

    to_parquet = make_parquet_task(
        endpoint="getComtradeReleases",
    )(json_key=validated)
