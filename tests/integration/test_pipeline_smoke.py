"""
Integration smoke tests for the Comtrade pipeline.

These tests exercise multiple components together against a moto-emulated S3
bucket — no real AWS credentials needed.  Unlike the unit tests (which mock
individual functions), these tests let the code actually read and write S3
objects, verifying that:

  - S3 keys are constructed correctly
  - JSON and Parquet serialization round-trips correctly
  - Validation and schema-drift modules read from S3 and interpret the data
  - The dead-letter manifest writer produces the expected S3 object structure
  - The full pipeline chain can run end-to-end without errors

Scope
-----
  - plugins/comtrade/s3_writer.py
  - plugins/comtrade/validator.py
  - plugins/comtrade/schema.py
  - plugins/comtrade/callbacks.py (write_error_manifest)

Excluded from scope (require pyiceberg/Airflow execution context):
  - iceberg.py  — covered by test_iceberg.py (sys.modules mocks)
  - dag_factory.py — covered by test_dag_factory.py and dag_integrity tests
"""
from __future__ import annotations

import json

import boto3
import pytest

pytestmark = pytest.mark.integration

BUCKET = "test-bucket"
REGION = "us-east-1"


# ═════════════════════════════════════════════════════════════════════════════
# 1 — Bronze layer: write JSON to S3, read it back
# ═════════════════════════════════════════════════════════════════════════════


class TestBronzeWriteAndRead:
    """
    Exercises the seam between the API response dict and S3 storage.
    Verifies key construction, serialization, content-type, and round-trip
    fidelity without mocking any S3 calls.
    """

    def test_s3_key_is_hive_partitioned(self, bronze_key):
        assert "comtrade/preview" in bronze_key
        assert "type=C" in bronze_key
        assert "freq=A" in bronze_key
        assert "year=2023" in bronze_key
        assert "month=01" in bronze_key
        assert bronze_key.endswith(".json")

    def test_object_exists_in_s3(self, aws, bronze_key):
        response = aws.head_object(Bucket=BUCKET, Key=bronze_key)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_content_type_is_json(self, aws, bronze_key):
        response = aws.head_object(Bucket=BUCKET, Key=bronze_key)
        assert response["ContentType"] == "application/json"

    def test_round_trip_preserves_record_count(self, aws, bronze_key, api_envelope):
        obj = aws.get_object(Bucket=BUCKET, Key=bronze_key)
        recovered = json.loads(obj["Body"].read())
        assert len(recovered["data"]) == len(api_envelope["data"])

    def test_round_trip_preserves_trade_values(self, aws, bronze_key, trade_records):
        obj = aws.get_object(Bucket=BUCKET, Key=bronze_key)
        recovered = json.loads(obj["Body"].read())
        original_values = {r["primaryValue"] for r in trade_records}
        recovered_values = {r["primaryValue"] for r in recovered["data"]}
        assert original_values == recovered_values

    def test_round_trip_preserves_all_columns(self, aws, bronze_key, trade_records):
        obj = aws.get_object(Bucket=BUCKET, Key=bronze_key)
        recovered = json.loads(obj["Body"].read())
        original_cols = set(trade_records[0].keys())
        recovered_cols = set(recovered["data"][0].keys())
        assert original_cols == recovered_cols

    def test_unicode_is_preserved(self, aws, mock_variables):
        """Non-ASCII characters in country names survive the S3 round-trip."""
        from comtrade.s3_writer import build_s3_key, write_json_to_s3

        data = {"data": [{"reporterDesc": "Côte d'Ivoire", "value": 1.0}]}
        key = build_s3_key("preview", "unicode-run", "2023", "01")
        write_json_to_s3(data, BUCKET, key)
        obj = aws.get_object(Bucket=BUCKET, Key=key)
        recovered = json.loads(obj["Body"].read())
        assert recovered["data"][0]["reporterDesc"] == "Côte d'Ivoire"


# ═════════════════════════════════════════════════════════════════════════════
# 2 — Validation layer: bronze JSON in S3 → validator → quality results
# ═════════════════════════════════════════════════════════════════════════════


class TestValidationPipeline:
    """
    Exercises the seam between S3 storage and the validator module.
    Reads the actual S3 object written by the extract step and runs quality
    checks against it — verifying that the JSON survives the serialization
    round-trip intact enough to pass all checks.
    """

    def test_all_checks_pass_on_valid_data(self, aws, bronze_key):
        from comtrade.validator import assert_quality, run_checks

        obj = aws.get_object(Bucket=BUCKET, Key=bronze_key)
        data = json.loads(obj["Body"].read())

        results = run_checks(
            data,
            required_columns=["reporterCode", "period"],
            numeric_columns=["primaryValue"],
            dedup_columns=["reporterCode", "partnerCode", "flowCode", "period"],
            min_rows=1,
            freq_code="A",
        )
        # assert_quality raises on any ERROR-severity failure
        assert_quality(results)

    def test_check_envelope_passes(self, aws, bronze_key):
        from comtrade.validator import check_envelope

        obj = aws.get_object(Bucket=BUCKET, Key=bronze_key)
        data = json.loads(obj["Body"].read())
        result = check_envelope(data)
        assert result.passed

    def test_check_has_data_key_passes(self, aws, bronze_key):
        from comtrade.validator import check_has_data_key

        obj = aws.get_object(Bucket=BUCKET, Key=bronze_key)
        data = json.loads(obj["Body"].read())
        result = check_has_data_key(data)
        assert result.passed

    def test_check_row_count_passes(self, aws, bronze_key):
        from comtrade.validator import check_row_count

        obj = aws.get_object(Bucket=BUCKET, Key=bronze_key)
        data = json.loads(obj["Body"].read())
        records = data["data"]
        result = check_row_count(records, min_rows=1)
        assert result.passed

    def test_bad_data_fails_envelope_check(self, aws, mock_variables):
        """A non-dict, non-list response (e.g. an error string) fails check_envelope."""
        from comtrade.s3_writer import build_s3_key, write_json_to_s3
        from comtrade.validator import check_envelope

        bad_key = build_s3_key("preview", "bad-run", "2023", "01")
        write_json_to_s3("this is an error string", BUCKET, bad_key)

        obj = aws.get_object(Bucket=BUCKET, Key=bad_key)
        data = json.loads(obj["Body"].read())
        result = check_envelope(data)
        assert not result.passed

    def test_empty_data_array_fails_row_count(self, aws, mock_variables):
        from comtrade.s3_writer import build_s3_key, write_json_to_s3
        from comtrade.validator import check_row_count

        empty_key = build_s3_key("preview", "empty-run", "2023", "01")
        write_json_to_s3({"data": []}, BUCKET, empty_key)

        obj = aws.get_object(Bucket=BUCKET, Key=empty_key)
        data = json.loads(obj["Body"].read())
        result = check_row_count(data["data"], min_rows=1)
        assert not result.passed


# ═════════════════════════════════════════════════════════════════════════════
# 3 — Parquet layer: bronze JSON → Parquet → readable DataFrame
# ═════════════════════════════════════════════════════════════════════════════


class TestParquetPipeline:
    """
    Exercises the seam between S3 bronze JSON and the Parquet conversion step.
    Reads the S3 bronze object, converts to Parquet, writes it back to S3, then
    reads the Parquet bytes and verifies the DataFrame is structurally correct.
    """

    def test_parquet_object_written_to_s3(self, aws, bronze_key, mock_variables):
        import io

        import pyarrow.parquet as pq

        from comtrade.s3_writer import build_s3_key, write_parquet_to_s3

        obj = aws.get_object(Bucket=BUCKET, Key=bronze_key)
        data = json.loads(obj["Body"].read())
        records = data["data"]

        parquet_key = build_s3_key(
            endpoint="preview",
            run_id="smoke-test-run",
            year="2023",
            month="01",
            typeCode="C",
            freqCode="A",
            fmt="parquet",
        )
        write_parquet_to_s3(records, BUCKET, parquet_key)

        pq_obj = aws.get_object(Bucket=BUCKET, Key=parquet_key)
        table = pq.read_table(io.BytesIO(pq_obj["Body"].read()))
        assert table.num_rows == len(records)

    def test_parquet_key_has_fmt_partition(self, aws, bronze_key, mock_variables):
        from comtrade.s3_writer import build_s3_key, write_parquet_to_s3

        obj = aws.get_object(Bucket=BUCKET, Key=bronze_key)
        records = json.loads(obj["Body"].read())["data"]

        parquet_key = build_s3_key("preview", "smoke-run", "2023", "01", fmt="parquet")
        write_parquet_to_s3(records, BUCKET, parquet_key)

        assert "fmt=parquet" in parquet_key
        assert parquet_key.endswith(".parquet")

    def test_parquet_preserves_column_names(self, aws, bronze_key, trade_records, mock_variables):
        import io

        import pyarrow.parquet as pq

        from comtrade.s3_writer import build_s3_key, write_parquet_to_s3

        obj = aws.get_object(Bucket=BUCKET, Key=bronze_key)
        records = json.loads(obj["Body"].read())["data"]

        key = build_s3_key("preview", "col-check", "2023", "01", fmt="parquet")
        write_parquet_to_s3(records, BUCKET, key)

        pq_obj = aws.get_object(Bucket=BUCKET, Key=key)
        table = pq.read_table(io.BytesIO(pq_obj["Body"].read()))
        parquet_columns = set(table.column_names)
        expected_columns = set(trade_records[0].keys())
        assert expected_columns.issubset(parquet_columns)

    def test_parquet_numeric_values_preserved(self, aws, bronze_key, trade_records, mock_variables):
        import io

        import pyarrow.parquet as pq

        from comtrade.s3_writer import build_s3_key, write_parquet_to_s3

        obj = aws.get_object(Bucket=BUCKET, Key=bronze_key)
        records = json.loads(obj["Body"].read())["data"]

        key = build_s3_key("preview", "value-check", "2023", "01", fmt="parquet")
        write_parquet_to_s3(records, BUCKET, key)

        pq_obj = aws.get_object(Bucket=BUCKET, Key=key)
        table = pq.read_table(io.BytesIO(pq_obj["Body"].read()))
        df = table.to_pandas()

        original_values = sorted(r["primaryValue"] for r in trade_records)
        recovered_values = sorted(df["primaryValue"].tolist())
        assert original_values == pytest.approx(recovered_values)


# ═════════════════════════════════════════════════════════════════════════════
# 4 — Schema drift: multi-run lifecycle with real S3 reads/writes
# ═════════════════════════════════════════════════════════════════════════════


class TestSchemaDriftPipeline:
    """
    Exercises the full schema drift lifecycle.  Each call to detect_and_alert
    reads from and writes to the moto S3 bucket — verifying that baselines are
    persisted correctly and drift is detected on column changes.
    """

    def test_first_run_saves_baseline_to_s3(self, aws, trade_records):
        from comtrade.schema import _schema_s3_key, detect_and_alert

        result = detect_and_alert(BUCKET, "preview", trade_records, run_id="run-1")
        assert result is None  # first run never returns drift

        # Baseline file must now exist in S3
        key = _schema_s3_key("preview")
        response = aws.get_object(Bucket=BUCKET, Key=key)
        payload = json.loads(response["Body"].read())
        assert payload["endpoint"] == "preview"
        assert set(payload["columns"]) == set(trade_records[0].keys())

    def test_second_run_same_schema_returns_none(self, aws, trade_records):
        from comtrade.schema import detect_and_alert

        detect_and_alert(BUCKET, "preview", trade_records, run_id="run-1")
        result = detect_and_alert(BUCKET, "preview", trade_records, run_id="run-2")
        assert result is None

    def test_added_column_detected_as_drift(self, aws, trade_records):
        from comtrade.schema import detect_and_alert

        # Establish baseline
        detect_and_alert(BUCKET, "preview", trade_records, run_id="run-1")

        # New run has an extra column
        new_records = [{**r, "newField": "x"} for r in trade_records]
        result = detect_and_alert(BUCKET, "preview", new_records, run_id="run-2")

        assert result is not None
        assert result.has_drift
        assert "newField" in result.added
        assert len(result.removed) == 0

    def test_removed_column_detected_as_drift(self, aws, trade_records):
        from comtrade.schema import detect_and_alert

        detect_and_alert(BUCKET, "preview", trade_records, run_id="run-1")

        # New run is missing a column
        reduced_records = [{k: v for k, v in r.items() if k != "cmdDesc"} for r in trade_records]
        result = detect_and_alert(BUCKET, "preview", reduced_records, run_id="run-2")

        assert result is not None
        assert "cmdDesc" in result.removed

    def test_baseline_updated_after_drift(self, aws, trade_records):
        from comtrade.schema import _schema_s3_key, detect_and_alert

        detect_and_alert(BUCKET, "preview", trade_records, run_id="run-1")

        new_records = [{**r, "newField": "x"} for r in trade_records]
        detect_and_alert(BUCKET, "preview", new_records, run_id="run-2")

        # Third run with new schema should see no drift (baseline updated)
        result = detect_and_alert(BUCKET, "preview", new_records, run_id="run-3")
        assert result is None

        # Baseline on S3 should contain newField
        key = _schema_s3_key("preview")
        payload = json.loads(aws.get_object(Bucket=BUCKET, Key=key)["Body"].read())
        assert "newField" in payload["columns"]

    def test_endpoints_have_independent_baselines(self, aws, trade_records):
        from comtrade.schema import detect_and_alert

        detect_and_alert(BUCKET, "preview", trade_records, run_id="run-1")

        # A different endpoint should save its own baseline
        result = detect_and_alert(BUCKET, "getMBS", trade_records, run_id="run-1")
        assert result is None  # first run for getMBS

    def test_empty_records_skips_detection(self, aws):
        from comtrade.schema import detect_and_alert

        result = detect_and_alert(BUCKET, "preview", [], run_id="run-1")
        assert result is None


# ═════════════════════════════════════════════════════════════════════════════
# 5 — Dead-letter: task failure → manifest JSON in S3
# ═════════════════════════════════════════════════════════════════════════════


class TestDeadLetterPipeline:
    """
    Exercises the seam between the failure callback and S3.
    Verifies that write_error_manifest writes a well-formed JSON manifest to
    the expected S3 key path.
    """

    def _make_context(self):
        from unittest import mock

        dag = mock.Mock()
        dag.dag_id = "comtrade_preview"
        ti = mock.Mock()
        ti.task_id = "extract_and_store_raw"
        ti.log_url = "http://airflow:8080/log"
        return {
            "dag": dag,
            "task_instance": ti,
            "run_id": "scheduled__2024-01-01T00:00:00+00:00",
            "execution_date": "2024-01-01T00:00:00+00:00",
            "exception": ValueError("API returned 503"),
        }

    def test_manifest_object_exists_in_s3(self, aws):
        from comtrade.callbacks import write_error_manifest

        context = self._make_context()
        write_error_manifest(context)

        objects = aws.list_objects_v2(Bucket=BUCKET, Prefix="comtrade/errors/")
        assert objects.get("KeyCount", 0) >= 1

    def test_manifest_key_contains_dag_id(self, aws):
        from comtrade.callbacks import write_error_manifest

        context = self._make_context()
        write_error_manifest(context)

        objects = aws.list_objects_v2(Bucket=BUCKET, Prefix="comtrade/errors/")
        keys = [o["Key"] for o in objects.get("Contents", [])]
        assert any("dag_id=comtrade_preview" in k for k in keys)

    def test_manifest_key_contains_task_id(self, aws):
        from comtrade.callbacks import write_error_manifest

        context = self._make_context()
        write_error_manifest(context)

        objects = aws.list_objects_v2(Bucket=BUCKET, Prefix="comtrade/errors/")
        keys = [o["Key"] for o in objects.get("Contents", [])]
        assert any("task_id=extract_and_store_raw" in k for k in keys)

    def test_manifest_content_is_valid_json(self, aws):
        from comtrade.callbacks import write_error_manifest

        context = self._make_context()
        write_error_manifest(context)

        objects = aws.list_objects_v2(Bucket=BUCKET, Prefix="comtrade/errors/")
        key = objects["Contents"][0]["Key"]
        obj = aws.get_object(Bucket=BUCKET, Key=key)
        manifest = json.loads(obj["Body"].read())
        assert isinstance(manifest, dict)

    def test_manifest_contains_required_fields(self, aws):
        from comtrade.callbacks import write_error_manifest

        context = self._make_context()
        write_error_manifest(context)

        objects = aws.list_objects_v2(Bucket=BUCKET, Prefix="comtrade/errors/")
        key = objects["Contents"][0]["Key"]
        manifest = json.loads(aws.get_object(Bucket=BUCKET, Key=key)["Body"].read())

        assert "dag_id" in manifest
        assert "task_id" in manifest
        assert "run_id" in manifest
        assert "exception" in manifest

    def test_manifest_exception_message_is_captured(self, aws):
        from comtrade.callbacks import write_error_manifest

        context = self._make_context()
        write_error_manifest(context)

        objects = aws.list_objects_v2(Bucket=BUCKET, Prefix="comtrade/errors/")
        key = objects["Contents"][0]["Key"]
        manifest = json.loads(aws.get_object(Bucket=BUCKET, Key=key)["Body"].read())

        assert "503" in manifest["exception"]


# ═════════════════════════════════════════════════════════════════════════════
# 6 — Full pipeline chain: all stages end-to-end in sequence
# ═════════════════════════════════════════════════════════════════════════════


class TestFullPipelineChain:
    """
    Runs all pipeline stages sequentially on the same mock data.
    Validates that the output of each stage is consumable by the next,
    mirroring what Airflow would orchestrate via XCom.
    """

    def test_extract_validate_parquet_chain(self, aws, api_envelope, trade_records, mock_variables):
        """
        Stage 1 — Extract: write bronze JSON to S3
        Stage 2 — Validate: read bronze JSON, run quality checks
        Stage 3 — Parquet: convert bronze to Parquet, write to S3
        """
        import io

        import pyarrow.parquet as pq

        from comtrade.s3_writer import build_s3_key, write_json_to_s3, write_parquet_to_s3
        from comtrade.validator import assert_quality, run_checks

        # ── Stage 1: Extract ──────────────────────────────────────────────────
        bronze_key = build_s3_key(
            endpoint="preview",
            run_id="full-chain-run",
            year="2023",
            month="06",
            typeCode="C",
            freqCode="A",
        )
        write_json_to_s3(api_envelope, BUCKET, bronze_key)

        # Verify bronze object exists
        aws.head_object(Bucket=BUCKET, Key=bronze_key)

        # ── Stage 2: Validate ─────────────────────────────────────────────────
        obj = aws.get_object(Bucket=BUCKET, Key=bronze_key)
        data = json.loads(obj["Body"].read())

        results = run_checks(
            data,
            required_columns=["reporterCode", "period"],
            numeric_columns=["primaryValue"],
            min_rows=1,
            freq_code="A",
        )
        assert_quality(results)  # raises on ERROR-severity failure

        # ── Stage 3: Parquet ──────────────────────────────────────────────────
        records = data["data"]
        parquet_key = build_s3_key(
            endpoint="preview",
            run_id="full-chain-run",
            year="2023",
            month="06",
            typeCode="C",
            freqCode="A",
            fmt="parquet",
        )
        write_parquet_to_s3(records, BUCKET, parquet_key)

        pq_obj = aws.get_object(Bucket=BUCKET, Key=parquet_key)
        table = pq.read_table(io.BytesIO(pq_obj["Body"].read()))

        assert table.num_rows == len(trade_records)

    def test_extract_schema_drift_validates_baseline(self, aws, api_envelope, trade_records):
        """
        Stage 1 — Extract: write bronze JSON
        Stage 2 — Schema drift: detect_and_alert saves baseline
        Stage 3 — Second run: schema unchanged → no drift
        Stage 4 — Third run: new column → drift detected
        """
        from comtrade.s3_writer import build_s3_key, write_json_to_s3
        from comtrade.schema import detect_and_alert

        # Stage 1
        key = build_s3_key("preview", "chain-drift-run", "2023", "06")
        write_json_to_s3(api_envelope, BUCKET, key)

        obj = aws.get_object(Bucket=BUCKET, Key=key)
        records = json.loads(obj["Body"].read())["data"]

        # Stage 2: first drift check saves baseline
        result = detect_and_alert(BUCKET, "preview", records, run_id="run-1")
        assert result is None

        # Stage 3: same schema → no drift
        result = detect_and_alert(BUCKET, "preview", records, run_id="run-2")
        assert result is None

        # Stage 4: add column → drift detected
        evolved = [{**r, "newApiField": "value"} for r in records]
        result = detect_and_alert(BUCKET, "preview", evolved, run_id="run-3")
        assert result is not None and result.has_drift
        assert "newApiField" in result.added
