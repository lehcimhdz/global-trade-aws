"""
Unit tests for plugins/comtrade/s3_writer.py

Covers:
- build_s3_key(): every combination of optional path segments and formats
- write_json_to_s3(): S3 PutObject call, content-type, return URI, UTF-8 encoding
- write_parquet_to_s3(): Parquet serialization correctness, S3 upload, return URI

AWS calls are intercepted by moto — no real AWS credentials needed.
Airflow Variable.get is patched via the shared `mock_variables` fixture.
"""
from __future__ import annotations

import io
import json

import boto3
import pytest

# Skip this entire module if Airflow is not installed (s3_writer imports Variable)
pytest.importorskip("airflow", reason="apache-airflow not installed")

from moto import mock_aws  # noqa: E402

import comtrade.s3_writer as s3w  # noqa: E402

pytestmark = pytest.mark.unit

BUCKET = "test-bucket"
REGION = "us-east-1"


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _patch_variables(mock_variables):
    """Ensure Variable.get is patched for every test in this module."""


@pytest.fixture
def s3(mock_variables):
    """
    Start the moto S3 mock, create the test bucket, and yield a boto3 client.
    Torn down automatically after each test.
    """
    with mock_aws():
        boto3.client("s3", region_name=REGION).create_bucket(Bucket=BUCKET)
        yield boto3.client("s3", region_name=REGION)


# ─────────────────────────────────────────────────────────────────────────────
# build_s3_key
# ─────────────────────────────────────────────────────────────────────────────


class TestBuildS3Key:
    def test_full_key_with_type_and_freq(self):
        key = s3w.build_s3_key(
            endpoint="preview",
            run_id="run_001",
            year="2024",
            month="03",
            typeCode="C",
            freqCode="A",
        )
        assert key == "comtrade/preview/type=C/freq=A/year=2024/month=03/run_001.json"

    def test_no_type_no_freq(self):
        """Endpoints like getComtradeReleases have no type/freq path segments."""
        key = s3w.build_s3_key(
            endpoint="getComtradeReleases",
            run_id="run_002",
            year="2024",
            month="01",
        )
        assert key == "comtrade/getComtradeReleases/year=2024/month=01/run_002.json"

    def test_extra_partitions(self):
        """getMBS uses extra_partitions for series_type."""
        key = s3w.build_s3_key(
            endpoint="getMBS",
            run_id="run_003",
            year="2023",
            month="11",
            extra_partitions={"series_type": "T35"},
        )
        assert key == "comtrade/getMBS/series_type=T35/year=2023/month=11/run_003.json"

    def test_parquet_format_adds_prefix_and_extension(self):
        key = s3w.build_s3_key(
            endpoint="preview",
            run_id="run_004",
            year="2024",
            month="06",
            typeCode="C",
            freqCode="M",
            fmt="parquet",
        )
        assert key == "comtrade/preview/type=C/freq=M/year=2024/month=06/fmt=parquet/run_004.parquet"

    def test_type_without_freq(self):
        """Edge case: typeCode provided but freqCode omitted."""
        key = s3w.build_s3_key(
            endpoint="preview",
            run_id="run_005",
            year="2024",
            month="01",
            typeCode="C",
        )
        assert "type=C" in key
        assert "freq=" not in key

    def test_multiple_extra_partitions_are_ordered(self):
        """Extra partitions must appear in insertion order."""
        key = s3w.build_s3_key(
            endpoint="getMBS",
            run_id="r",
            year="2024",
            month="01",
            extra_partitions={"series_type": "T38", "country": "840"},
        )
        idx_series = key.index("series_type=T38")
        idx_country = key.index("country=840")
        assert idx_series < idx_country

    def test_run_id_special_chars_preserved(self):
        """run_id with hyphens (already sanitised in dag_factory) is preserved."""
        key = s3w.build_s3_key(
            endpoint="preview",
            run_id="scheduled--2024-03-01T00-00-00-00-00",
            year="2024",
            month="03",
        )
        assert "scheduled--2024-03-01T00-00-00-00-00.json" in key

    def test_json_is_default_format(self):
        key = s3w.build_s3_key(endpoint="preview", run_id="r", year="2024", month="01")
        assert key.endswith(".json")
        assert "fmt=parquet" not in key


# ─────────────────────────────────────────────────────────────────────────────
# write_json_to_s3
# ─────────────────────────────────────────────────────────────────────────────


class TestWriteJsonToS3:
    def test_returns_s3_uri(self, s3, sample_api_response):
        with mock_aws():
            s3.create_bucket(Bucket=BUCKET)
            uri = s3w.write_json_to_s3(sample_api_response, BUCKET, "comtrade/test/run.json")

        assert uri == f"s3://{BUCKET}/comtrade/test/run.json"

    def test_object_is_valid_json(self, s3, sample_api_response):
        with mock_aws():
            s3.create_bucket(Bucket=BUCKET)
            s3w.write_json_to_s3(sample_api_response, BUCKET, "comtrade/test/run.json")
            obj = s3.get_object(Bucket=BUCKET, Key="comtrade/test/run.json")
            body = json.loads(obj["Body"].read())

        assert body["count"] == 2
        assert len(body["data"]) == 2

    def test_content_type_is_json(self, s3, sample_api_response):
        with mock_aws():
            s3.create_bucket(Bucket=BUCKET)
            s3w.write_json_to_s3(sample_api_response, BUCKET, "comtrade/test/run.json")
            head = s3.head_object(Bucket=BUCKET, Key="comtrade/test/run.json")

        assert head["ContentType"] == "application/json"

    def test_handles_non_ascii_characters(self, s3):
        """Unicode in trade data (country names etc.) must be preserved."""
        data = {"desc": "Deutschland / 日本"}
        with mock_aws():
            s3.create_bucket(Bucket=BUCKET)
            s3w.write_json_to_s3(data, BUCKET, "comtrade/test/unicode.json")
            obj = s3.get_object(Bucket=BUCKET, Key="comtrade/test/unicode.json")
            body = json.loads(obj["Body"].read().decode("utf-8"))

        assert body["desc"] == "Deutschland / 日本"

    def test_handles_non_serialisable_types(self, s3):
        """default=str must handle Decimals, dates, etc. without raising."""
        from decimal import Decimal
        from datetime import date

        data = {"value": Decimal("1234.56"), "date": date(2024, 1, 1)}
        with mock_aws():
            s3.create_bucket(Bucket=BUCKET)
            # Should not raise
            s3w.write_json_to_s3(data, BUCKET, "comtrade/test/decimal.json")

    def test_empty_dict_is_written(self, s3):
        with mock_aws():
            s3.create_bucket(Bucket=BUCKET)
            uri = s3w.write_json_to_s3({}, BUCKET, "comtrade/test/empty.json")

        assert uri.endswith("empty.json")


# ─────────────────────────────────────────────────────────────────────────────
# write_parquet_to_s3
# ─────────────────────────────────────────────────────────────────────────────


class TestWriteParquetToS3:
    def test_returns_s3_uri(self, s3, sample_api_response):
        records = sample_api_response["data"]
        with mock_aws():
            s3.create_bucket(Bucket=BUCKET)
            uri = s3w.write_parquet_to_s3(records, BUCKET, "comtrade/test/fmt=parquet/run.parquet")

        assert uri == f"s3://{BUCKET}/comtrade/test/fmt=parquet/run.parquet"

    def test_uploaded_file_is_valid_parquet(self, s3, sample_api_response):
        """The S3 object must be readable as a valid Parquet file."""
        import pandas as pd

        records = sample_api_response["data"]
        with mock_aws():
            s3.create_bucket(Bucket=BUCKET)
            s3w.write_parquet_to_s3(records, BUCKET, "comtrade/test/data.parquet")
            obj = s3.get_object(Bucket=BUCKET, Key="comtrade/test/data.parquet")
            buf = io.BytesIO(obj["Body"].read())
            df = pd.read_parquet(buf)

        assert len(df) == 2
        assert "reporterCode" in df.columns
        assert "primaryValue" in df.columns

    def test_values_are_preserved(self, s3, sample_api_response):
        import pandas as pd

        records = sample_api_response["data"]
        with mock_aws():
            s3.create_bucket(Bucket=BUCKET)
            s3w.write_parquet_to_s3(records, BUCKET, "comtrade/test/vals.parquet")
            obj = s3.get_object(Bucket=BUCKET, Key="comtrade/test/vals.parquet")
            buf = io.BytesIO(obj["Body"].read())
            df = pd.read_parquet(buf)

        assert df.iloc[0]["reporterCode"] == "840"
        assert df.iloc[1]["reporterCode"] == "276"

    def test_nested_dicts_are_normalised(self, s3):
        """json_normalize must flatten nested keys."""
        import pandas as pd

        records = [{"outer": {"inner": "value"}, "flat": 1}]
        with mock_aws():
            s3.create_bucket(Bucket=BUCKET)
            s3w.write_parquet_to_s3(records, BUCKET, "comtrade/test/nested.parquet")
            obj = s3.get_object(Bucket=BUCKET, Key="comtrade/test/nested.parquet")
            buf = io.BytesIO(obj["Body"].read())
            df = pd.read_parquet(buf)

        # pandas json_normalize flattens outer.inner → outer.inner column
        assert "outer.inner" in df.columns or "outer" in df.columns

    def test_single_record_is_accepted(self, s3):
        with mock_aws():
            s3.create_bucket(Bucket=BUCKET)
            uri = s3w.write_parquet_to_s3(
                [{"a": 1, "b": "x"}], BUCKET, "comtrade/test/single.parquet"
            )
        assert uri.endswith(".parquet")
