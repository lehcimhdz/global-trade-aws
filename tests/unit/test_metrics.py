"""
Unit tests for plugins/comtrade/metrics.py

The module has no Airflow dependency at import time. These tests cover:
  - _build_metric_data: correct metric names, values, and dimensions
  - emit_validation_metrics: CloudWatch put_metric_data is called correctly
  - Error isolation: CloudWatch failures must not propagate
"""
from __future__ import annotations

from typing import Any
from unittest import mock

import pytest

from comtrade.metrics import (
    NAMESPACE,
    _build_metric_data,
    emit_validation_metrics,
)


# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_result(name: str, passed: bool, row_count: int | None = None):
    r = mock.Mock()
    r.name = name
    r.passed = passed
    r.details = {"row_count": row_count} if row_count is not None else {}
    return r


def _make_results(row_count: int = 42, passed: int = 5, failed: int = 1):
    results = []
    results.append(_make_result("check_row_count", True, row_count=row_count))
    for _ in range(passed - 1):  # check_row_count already counts as 1 passed
        results.append(_make_result("check_envelope", True))
    for _ in range(failed):
        results.append(_make_result("check_no_nulls", False))
    return results


# ── _build_metric_data ────────────────────────────────────────────────────────


class TestBuildMetricData:
    def test_returns_three_metrics_without_bytes(self):
        results = _make_results()
        data = _build_metric_data("my_dag", "tariffline", results, json_bytes=None)
        names = {m["MetricName"] for m in data}
        assert names == {"RowCount", "ChecksPassed", "ChecksFailed"}

    def test_returns_four_metrics_with_bytes(self):
        results = _make_results()
        data = _build_metric_data("my_dag", "tariffline", results, json_bytes=1024)
        names = {m["MetricName"] for m in data}
        assert names == {"RowCount", "ChecksPassed", "ChecksFailed", "JsonBytesWritten"}

    def test_row_count_extracted_from_check_row_count(self):
        results = _make_results(row_count=99)
        data = _build_metric_data("d", "e", results, json_bytes=None)
        row = next(m for m in data if m["MetricName"] == "RowCount")
        assert row["Value"] == 99.0

    def test_row_count_zero_when_no_check_row_count_result(self):
        results = [_make_result("check_envelope", True)]
        data = _build_metric_data("d", "e", results, json_bytes=None)
        row = next(m for m in data if m["MetricName"] == "RowCount")
        assert row["Value"] == 0.0

    def test_checks_passed_count(self):
        results = _make_results(row_count=10, passed=4, failed=2)
        data = _build_metric_data("d", "e", results, json_bytes=None)
        p = next(m for m in data if m["MetricName"] == "ChecksPassed")
        assert p["Value"] == 4.0

    def test_checks_failed_count(self):
        results = _make_results(row_count=10, passed=4, failed=2)
        data = _build_metric_data("d", "e", results, json_bytes=None)
        f = next(m for m in data if m["MetricName"] == "ChecksFailed")
        assert f["Value"] == 2.0

    def test_json_bytes_value(self):
        results = _make_results()
        data = _build_metric_data("d", "e", results, json_bytes=2048)
        b = next(m for m in data if m["MetricName"] == "JsonBytesWritten")
        assert b["Value"] == 2048.0
        assert b["Unit"] == "Bytes"

    def test_dimensions_contain_dag_id_and_endpoint(self):
        results = _make_results()
        data = _build_metric_data("comtrade_preview", "tariffline", results, json_bytes=None)
        for metric in data:
            dim_map = {d["Name"]: d["Value"] for d in metric["Dimensions"]}
            assert dim_map["DagId"] == "comtrade_preview"
            assert dim_map["Endpoint"] == "tariffline"

    def test_all_metrics_have_count_unit_except_bytes(self):
        results = _make_results()
        data = _build_metric_data("d", "e", results, json_bytes=512)
        for metric in data:
            if metric["MetricName"] == "JsonBytesWritten":
                assert metric["Unit"] == "Bytes"
            else:
                assert metric["Unit"] == "Count"

    def test_all_checks_pass_zero_failed(self):
        results = [
            _make_result("check_row_count", True, row_count=5),
            _make_result("check_envelope", True),
            _make_result("check_has_data_key", True),
        ]
        data = _build_metric_data("d", "e", results, json_bytes=None)
        f = next(m for m in data if m["MetricName"] == "ChecksFailed")
        p = next(m for m in data if m["MetricName"] == "ChecksPassed")
        assert f["Value"] == 0.0
        assert p["Value"] == 3.0

    def test_empty_results_list(self):
        data = _build_metric_data("d", "e", [], json_bytes=None)
        f = next(m for m in data if m["MetricName"] == "ChecksFailed")
        p = next(m for m in data if m["MetricName"] == "ChecksPassed")
        assert f["Value"] == 0.0
        assert p["Value"] == 0.0


# ── emit_validation_metrics ───────────────────────────────────────────────────


class TestEmitValidationMetrics:
    def _mock_cw(self):
        cw = mock.MagicMock()
        client = mock.MagicMock()
        cw.return_value = client
        return cw, client

    def test_calls_put_metric_data(self):
        cw_mock, cw_client = self._mock_cw()
        results = _make_results()
        with mock.patch("boto3.client", cw_mock):
            emit_validation_metrics("dag_x", "tariffline", results)
        cw_client.put_metric_data.assert_called_once()

    def test_namespace_is_correct(self):
        cw_mock, cw_client = self._mock_cw()
        results = _make_results()
        with mock.patch("boto3.client", cw_mock):
            emit_validation_metrics("dag_x", "tariffline", results)
        kwargs = cw_client.put_metric_data.call_args.kwargs
        assert kwargs["Namespace"] == NAMESPACE

    def test_metric_data_passed_to_cloudwatch(self):
        cw_mock, cw_client = self._mock_cw()
        results = _make_results(row_count=7, passed=3, failed=1)
        with mock.patch("boto3.client", cw_mock):
            emit_validation_metrics("dag_x", "ep", results, json_bytes=500)
        kwargs = cw_client.put_metric_data.call_args.kwargs
        metric_names = {m["MetricName"] for m in kwargs["MetricData"]}
        assert "RowCount" in metric_names
        assert "ChecksPassed" in metric_names
        assert "ChecksFailed" in metric_names
        assert "JsonBytesWritten" in metric_names

    def test_cloudwatch_error_does_not_propagate(self):
        results = _make_results()
        with mock.patch("boto3.client", side_effect=RuntimeError("no credentials")):
            emit_validation_metrics("dag_x", "ep", results)  # must not raise

    def test_uses_default_region_when_airflow_absent(self):
        import sys

        cw_mock, _ = self._mock_cw()
        saved = {k: v for k, v in sys.modules.items() if k.startswith("airflow")}
        for key in saved:
            sys.modules[key] = None  # type: ignore[assignment]
        try:
            with mock.patch("boto3.client", cw_mock):
                emit_validation_metrics("d", "e", _make_results())
            call_kwargs = cw_mock.call_args
            assert call_kwargs.kwargs.get("region_name") == "us-east-1"
        finally:
            for key in saved:
                sys.modules[key] = saved[key]

    def test_logs_success(self, caplog):
        import logging

        cw_mock, _ = self._mock_cw()
        with mock.patch("boto3.client", cw_mock):
            with caplog.at_level(logging.INFO, logger="comtrade.metrics"):
                emit_validation_metrics("dag_x", "ep", _make_results(row_count=3))
        assert any("metrics emitted" in r.message for r in caplog.records)

    def test_logs_error_on_failure(self, caplog):
        import logging

        with mock.patch("boto3.client", side_effect=Exception("oops")):
            with caplog.at_level(logging.ERROR, logger="comtrade.metrics"):
                emit_validation_metrics("d", "e", _make_results())
        assert any("Failed to emit" in r.message for r in caplog.records)

    def test_json_bytes_none_omits_metric(self):
        cw_mock, cw_client = self._mock_cw()
        results = _make_results()
        with mock.patch("boto3.client", cw_mock):
            emit_validation_metrics("d", "e", results, json_bytes=None)
        kwargs = cw_client.put_metric_data.call_args.kwargs
        names = {m["MetricName"] for m in kwargs["MetricData"]}
        assert "JsonBytesWritten" not in names
