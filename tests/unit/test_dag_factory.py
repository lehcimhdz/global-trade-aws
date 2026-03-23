"""
Unit tests for plugins/comtrade/dag_factory.py

Strategy
--------
`make_extract_task` and `make_parquet_task` return `@task`-decorated functions.
In Airflow 2.x the TaskFlow `@task` decorator wraps the original callable and
exposes it via the ``function`` attribute (and ``__wrapped__``).  We call
``task_obj.function`` to get the raw Python callable, then invoke it directly
with a synthetic Airflow task-context dict.

This tests the *business logic* (API call → S3 write → return key) without
spinning up an Airflow scheduler or DB.

Covers:
- extract task: API called, S3 key built correctly, JSON written, key returned
- extract task: run_id special characters are sanitised
- parquet task: skipped when COMTRADE_WRITE_PARQUET != "true"
- parquet task: reads JSON from S3, extracts data[], writes Parquet
- parquet task: graceful handling of empty data array
- parquet task: graceful handling of non-dict API response
- _bucket() / _parquet_enabled() helpers
"""
from __future__ import annotations

import json
from datetime import datetime
from unittest import mock

import pytest

# Skip this entire module if Airflow is not installed
pytest.importorskip("airflow", reason="apache-airflow not installed")

pytestmark = pytest.mark.unit

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

_LOGICAL_DATE = datetime(2024, 3, 1, 0, 0, 0)
_RUN_ID_RAW = "scheduled:2024-03-01T00:00:00+00:00"
_RUN_ID_CLEAN = "scheduled-2024-03-01T00-00-00-00-00"


def _make_context(run_id: str = _RUN_ID_RAW, logical_date: datetime = _LOGICAL_DATE) -> dict:
    return {"run_id": run_id, "logical_date": logical_date}


def _unwrap(task_obj):
    """Return the raw Python callable from an Airflow @task-decorated object."""
    if hasattr(task_obj, "function"):
        return task_obj.function
    if hasattr(task_obj, "__wrapped__"):
        return task_obj.__wrapped__
    # Last resort: the object itself may be callable (e.g. if @task was mocked)
    return task_obj


# ─────────────────────────────────────────────────────────────────────────────
# Imports — done after conftest sets env vars
# ─────────────────────────────────────────────────────────────────────────────

from comtrade.dag_factory import _bucket, _parquet_enabled, make_extract_task, make_parquet_task  # noqa: E402


# ─────────────────────────────────────────────────────────────────────────────
# _bucket / _parquet_enabled helpers
# ─────────────────────────────────────────────────────────────────────────────


class TestHelpers:
    def test_bucket_reads_variable(self, mock_variables):
        assert _bucket() == "test-bucket"

    def test_parquet_disabled_by_default(self, mock_variables):
        assert _parquet_enabled() is False

    def test_parquet_enabled_when_variable_is_true(self, mock_variables):
        mock_variables["COMTRADE_WRITE_PARQUET"] = "true"
        assert _parquet_enabled() is True

    def test_parquet_case_insensitive(self, mock_variables):
        mock_variables["COMTRADE_WRITE_PARQUET"] = "TRUE"
        assert _parquet_enabled() is True

    def test_parquet_disabled_for_false_string(self, mock_variables):
        mock_variables["COMTRADE_WRITE_PARQUET"] = "false"
        assert _parquet_enabled() is False


# ─────────────────────────────────────────────────────────────────────────────
# make_extract_task
# ─────────────────────────────────────────────────────────────────────────────


class TestExtractTask:
    def _make(self, api_fn, api_kwargs_fn=None, **factory_kwargs):
        if api_kwargs_fn is None:
            api_kwargs_fn = lambda: {}
        task_obj = make_extract_task(
            endpoint="preview",
            api_fn=api_fn,
            api_kwargs_fn=api_kwargs_fn,
            typeCode="C",
            freqCode="A",
            **factory_kwargs,
        )
        return _unwrap(task_obj)

    def test_calls_api_function(self, mock_variables, sample_api_response):
        mock_api = mock.Mock(return_value=sample_api_response)
        fn = self._make(mock_api, lambda: {"typeCode": "C", "freqCode": "A", "clCode": "HS"})

        with mock.patch("comtrade.dag_factory.write_json_to_s3", return_value="s3://b/k"):
            fn(**_make_context())

        mock_api.assert_called_once_with(typeCode="C", freqCode="A", clCode="HS")

    def test_returns_s3_key(self, mock_variables, sample_api_response):
        mock_api = mock.Mock(return_value=sample_api_response)
        fn = self._make(mock_api)

        with mock.patch("comtrade.dag_factory.write_json_to_s3") as mock_write:
            mock_write.return_value = "s3://test-bucket/comtrade/preview/run.json"
            result = fn(**_make_context())

        assert result is not None
        assert isinstance(result, str)

    def test_s3_key_contains_endpoint(self, mock_variables, sample_api_response):
        mock_api = mock.Mock(return_value=sample_api_response)
        fn = self._make(mock_api)

        captured_key = {}

        def _capture(data, bucket, key):
            captured_key["key"] = key
            return f"s3://{bucket}/{key}"

        with mock.patch("comtrade.dag_factory.write_json_to_s3", side_effect=_capture):
            fn(**_make_context())

        assert "preview" in captured_key["key"]

    def test_s3_key_contains_year_and_month(self, mock_variables, sample_api_response):
        mock_api = mock.Mock(return_value=sample_api_response)
        fn = self._make(mock_api)
        captured = {}

        def _capture(data, bucket, key):
            captured["key"] = key
            return f"s3://b/{key}"

        with mock.patch("comtrade.dag_factory.write_json_to_s3", side_effect=_capture):
            fn(**_make_context(logical_date=datetime(2024, 7, 15)))

        assert "year=2024" in captured["key"]
        assert "month=07" in captured["key"]

    def test_run_id_colons_are_sanitised(self, mock_variables, sample_api_response):
        """Colons and + signs in run_id must be replaced with hyphens."""
        mock_api = mock.Mock(return_value=sample_api_response)
        fn = self._make(mock_api)
        captured = {}

        def _capture(data, bucket, key):
            captured["key"] = key
            return f"s3://b/{key}"

        with mock.patch("comtrade.dag_factory.write_json_to_s3", side_effect=_capture):
            fn(**_make_context(run_id="manual:run+id"))

        assert ":" not in captured["key"]
        assert "+" not in captured["key"]

    def test_write_called_with_correct_bucket(self, mock_variables, sample_api_response):
        mock_api = mock.Mock(return_value=sample_api_response)
        fn = self._make(mock_api)
        captured = {}

        def _capture(data, bucket, key):
            captured["bucket"] = bucket
            return f"s3://{bucket}/{key}"

        with mock.patch("comtrade.dag_factory.write_json_to_s3", side_effect=_capture):
            fn(**_make_context())

        assert captured["bucket"] == "test-bucket"

    def test_api_response_forwarded_to_writer(self, mock_variables, sample_api_response):
        mock_api = mock.Mock(return_value=sample_api_response)
        fn = self._make(mock_api)
        captured = {}

        def _capture(data, bucket, key):
            captured["data"] = data
            return f"s3://b/{key}"

        with mock.patch("comtrade.dag_factory.write_json_to_s3", side_effect=_capture):
            fn(**_make_context())

        assert captured["data"] == sample_api_response

    def test_extra_partitions_appear_in_key(self, mock_variables, sample_api_response):
        mock_api = mock.Mock(return_value=sample_api_response)
        task_obj = make_extract_task(
            endpoint="getMBS",
            api_fn=mock_api,
            api_kwargs_fn=lambda: {},
            extra_partitions={"series_type": "T35"},
        )
        fn = _unwrap(task_obj)
        captured = {}

        def _capture(data, bucket, key):
            captured["key"] = key
            return f"s3://b/{key}"

        with mock.patch("comtrade.dag_factory.write_json_to_s3", side_effect=_capture):
            fn(**_make_context())

        assert "series_type=T35" in captured["key"]


# ─────────────────────────────────────────────────────────────────────────────
# make_parquet_task
# ─────────────────────────────────────────────────────────────────────────────


class TestParquetTask:
    def _make(self, **factory_kwargs):
        task_obj = make_parquet_task(endpoint="preview", typeCode="C", freqCode="A", **factory_kwargs)
        return _unwrap(task_obj)

    def test_returns_none_when_parquet_disabled(self, mock_variables):
        mock_variables["COMTRADE_WRITE_PARQUET"] = "false"
        fn = self._make()
        result = fn(json_key="comtrade/preview/run.json", **_make_context())
        assert result is None

    def test_skips_s3_read_when_disabled(self, mock_variables):
        mock_variables["COMTRADE_WRITE_PARQUET"] = "false"
        fn = self._make()

        with mock.patch("comtrade.dag_factory.boto3") as mock_boto:
            fn(json_key="comtrade/preview/run.json", **_make_context())

        mock_boto.client.assert_not_called()

    def test_reads_json_from_s3_when_enabled(self, mock_variables, sample_api_response):
        mock_variables["COMTRADE_WRITE_PARQUET"] = "true"
        fn = self._make()

        raw_bytes = json.dumps(sample_api_response).encode()
        mock_s3 = mock.Mock()
        mock_s3.get_object.return_value = {"Body": mock.Mock(read=mock.Mock(return_value=raw_bytes))}

        with mock.patch("comtrade.dag_factory.boto3") as mock_boto, \
             mock.patch("comtrade.dag_factory.write_parquet_to_s3", return_value="s3://b/p"):
            mock_boto.client.return_value = mock_s3
            fn(json_key="comtrade/preview/run.json", **_make_context())

        mock_s3.get_object.assert_called_once_with(Bucket="test-bucket", Key="comtrade/preview/run.json")

    def test_extracts_data_array_from_envelope(self, mock_variables, sample_api_response):
        mock_variables["COMTRADE_WRITE_PARQUET"] = "true"
        fn = self._make()

        raw_bytes = json.dumps(sample_api_response).encode()
        mock_s3 = mock.Mock()
        mock_s3.get_object.return_value = {"Body": mock.Mock(read=mock.Mock(return_value=raw_bytes))}
        captured = {}

        def _capture(records, bucket, key):
            captured["records"] = records
            return f"s3://b/{key}"

        with mock.patch("comtrade.dag_factory.boto3") as mock_boto, \
             mock.patch("comtrade.dag_factory.write_parquet_to_s3", side_effect=_capture):
            mock_boto.client.return_value = mock_s3
            fn(json_key="comtrade/preview/run.json", **_make_context())

        assert captured["records"] == sample_api_response["data"]
        assert len(captured["records"]) == 2

    def test_returns_none_for_empty_data_array(self, mock_variables):
        """If the API response has an empty data list, skip Parquet and return None."""
        mock_variables["COMTRADE_WRITE_PARQUET"] = "true"
        fn = self._make()

        raw_bytes = json.dumps({"elapsedMs": 1, "count": 0, "data": []}).encode()
        mock_s3 = mock.Mock()
        mock_s3.get_object.return_value = {"Body": mock.Mock(read=mock.Mock(return_value=raw_bytes))}

        with mock.patch("comtrade.dag_factory.boto3") as mock_boto, \
             mock.patch("comtrade.dag_factory.write_parquet_to_s3") as mock_write:
            mock_boto.client.return_value = mock_s3
            result = fn(json_key="comtrade/preview/run.json", **_make_context())

        assert result is None
        mock_write.assert_not_called()

    def test_returns_none_for_non_list_data(self, mock_variables):
        """If data is not a list (e.g. a dict), skip Parquet."""
        mock_variables["COMTRADE_WRITE_PARQUET"] = "true"
        fn = self._make()

        raw_bytes = json.dumps({"data": {"key": "value"}}).encode()
        mock_s3 = mock.Mock()
        mock_s3.get_object.return_value = {"Body": mock.Mock(read=mock.Mock(return_value=raw_bytes))}

        with mock.patch("comtrade.dag_factory.boto3") as mock_boto, \
             mock.patch("comtrade.dag_factory.write_parquet_to_s3") as mock_write:
            mock_boto.client.return_value = mock_s3
            result = fn(json_key="comtrade/preview/run.json", **_make_context())

        assert result is None
        mock_write.assert_not_called()

    def test_parquet_key_has_fmt_parquet_segment(self, mock_variables, sample_api_response):
        mock_variables["COMTRADE_WRITE_PARQUET"] = "true"
        fn = self._make()

        raw_bytes = json.dumps(sample_api_response).encode()
        mock_s3 = mock.Mock()
        mock_s3.get_object.return_value = {"Body": mock.Mock(read=mock.Mock(return_value=raw_bytes))}
        captured = {}

        def _capture(records, bucket, key):
            captured["key"] = key
            return f"s3://b/{key}"

        with mock.patch("comtrade.dag_factory.boto3") as mock_boto, \
             mock.patch("comtrade.dag_factory.write_parquet_to_s3", side_effect=_capture):
            mock_boto.client.return_value = mock_s3
            fn(json_key="comtrade/preview/run.json", **_make_context())

        assert "fmt=parquet" in captured["key"]
        assert captured["key"].endswith(".parquet")

    def test_response_without_data_key_is_used_directly(self, mock_variables):
        """If the response is a list (no envelope), use it as-is."""
        mock_variables["COMTRADE_WRITE_PARQUET"] = "true"
        fn = self._make()

        records = [{"a": 1}, {"a": 2}]
        raw_bytes = json.dumps(records).encode()
        mock_s3 = mock.Mock()
        mock_s3.get_object.return_value = {"Body": mock.Mock(read=mock.Mock(return_value=raw_bytes))}
        captured = {}

        def _capture(recs, bucket, key):
            captured["records"] = recs
            return f"s3://b/{key}"

        with mock.patch("comtrade.dag_factory.boto3") as mock_boto, \
             mock.patch("comtrade.dag_factory.write_parquet_to_s3", side_effect=_capture):
            mock_boto.client.return_value = mock_s3
            fn(json_key="comtrade/preview/run.json", **_make_context())

        assert captured["records"] == records
