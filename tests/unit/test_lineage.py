"""
Unit tests for plugins/comtrade/lineage.py

The module has no Airflow dependency at import time.  These tests cover:
  - _run_uuid: deterministic UUID derivation
  - _split_uri: S3 and HTTP namespace/name splitting
  - _build_run_event: correct OpenLineage payload structure
  - emit_task_complete: HTTP POST called correctly, error isolation
"""
from __future__ import annotations

import json
import uuid
from unittest import mock

import pytest

from comtrade.lineage import (
    _build_run_event,
    _run_uuid,
    _split_uri,
    emit_task_complete,
)


# ── _run_uuid ─────────────────────────────────────────────────────────────────


class TestRunUuid:
    def test_returns_valid_uuid_string(self):
        result = _run_uuid("scheduled__2024-01-01T00:00:00+00:00")
        uuid.UUID(result)  # raises ValueError if not valid

    def test_same_input_produces_same_output(self):
        run_id = "scheduled__2024-03-01T00:00:00+00:00"
        assert _run_uuid(run_id) == _run_uuid(run_id)

    def test_different_inputs_produce_different_uuids(self):
        assert _run_uuid("run_a") != _run_uuid("run_b")


# ── _split_uri ────────────────────────────────────────────────────────────────


class TestSplitUri:
    def test_s3_uri_bucket_and_key(self):
        ns, name = _split_uri("s3://my-bucket/comtrade/preview/year=2024/run.json")
        assert ns == "s3://my-bucket"
        assert name == "comtrade/preview/year=2024/run.json"

    def test_s3_uri_no_key(self):
        ns, name = _split_uri("s3://my-bucket")
        assert ns == "s3://my-bucket"
        assert name == "/"

    def test_https_uri_splits_host_and_path(self):
        ns, name = _split_uri("https://comtradeapi.un.org/public/v1/preview")
        assert ns == "https://comtradeapi.un.org"
        assert name == "/public/v1/preview"

    def test_http_uri(self):
        ns, name = _split_uri("http://marquez:5000/api/v1/lineage")
        assert ns == "http://marquez:5000"
        assert name == "/api/v1/lineage"

    def test_unknown_scheme_returns_uri_as_namespace(self):
        ns, name = _split_uri("custom://resource")
        assert ns == "custom://resource"
        assert name == "/"


# ── _build_run_event ──────────────────────────────────────────────────────────


class TestBuildRunEvent:
    def _event(self, **kwargs):
        defaults = dict(
            event_type="COMPLETE",
            dag_id="comtrade_preview",
            task_id="extract_and_store_raw",
            run_id="scheduled__2024-01-01",
            input_uris=["https://comtradeapi.un.org/public/v1/preview"],
            output_uris=["s3://my-bucket/comtrade/preview/year=2024/run.json"],
        )
        defaults.update(kwargs)
        return _build_run_event(**defaults)

    def test_event_type_set(self):
        assert self._event(event_type="COMPLETE")["eventType"] == "COMPLETE"

    def test_start_event_type(self):
        assert self._event(event_type="START")["eventType"] == "START"

    def test_run_id_is_uuid(self):
        event = self._event()
        uuid.UUID(event["run"]["runId"])  # must be valid UUID

    def test_job_namespace_is_comtrade(self):
        assert self._event()["job"]["namespace"] == "comtrade"

    def test_job_name_combines_dag_and_task(self):
        event = self._event(dag_id="comtrade_mbs", task_id="validate_bronze")
        assert event["job"]["name"] == "comtrade_mbs.validate_bronze"

    def test_inputs_split_correctly(self):
        event = self._event(input_uris=["s3://bucket/comtrade/preview/run.json"])
        assert event["inputs"][0]["namespace"] == "s3://bucket"
        assert event["inputs"][0]["name"] == "comtrade/preview/run.json"

    def test_outputs_split_correctly(self):
        event = self._event(output_uris=["s3://bucket/comtrade/preview/run.json"])
        assert event["outputs"][0]["namespace"] == "s3://bucket"
        assert event["outputs"][0]["name"] == "comtrade/preview/run.json"

    def test_empty_outputs(self):
        event = self._event(output_uris=[])
        assert event["outputs"] == []

    def test_producer_field_present(self):
        assert "producer" in self._event()

    def test_schema_url_present(self):
        assert "schemaURL" in self._event()

    def test_event_time_present(self):
        assert "eventTime" in self._event()

    def test_multiple_inputs(self):
        uris = ["s3://b/a.json", "https://api.example.com/v1/ep"]
        event = self._event(input_uris=uris)
        assert len(event["inputs"]) == 2


# ── emit_task_complete ────────────────────────────────────────────────────────


class TestEmitTaskComplete:
    def _fake_airflow_modules(self, url="http://marquez:5000"):
        import sys

        fake_variable = mock.MagicMock()
        fake_variable.get.return_value = url
        fake_models = mock.MagicMock()
        fake_models.Variable = fake_variable
        fake_airflow = mock.MagicMock()
        fake_airflow.models = fake_models
        return mock.patch.dict(
            sys.modules,
            {"airflow": fake_airflow, "airflow.models": fake_models},
        )

    def _mock_urlopen(self, status=201):
        resp = mock.MagicMock()
        resp.status = status
        resp.__enter__ = mock.Mock(return_value=resp)
        resp.__exit__ = mock.Mock(return_value=False)
        return mock.patch("urllib.request.urlopen", return_value=resp)

    def test_posts_to_lineage_endpoint(self):
        with self._fake_airflow_modules(), self._mock_urlopen() as mock_open:
            emit_task_complete("dag_x", "task_y", "run_1",
                               ["https://api/ep"], ["s3://bucket/key.json"])
        request = mock_open.call_args[0][0]
        assert request.full_url == "http://marquez:5000/api/v1/lineage"

    def test_posts_valid_json(self):
        with self._fake_airflow_modules(), self._mock_urlopen() as mock_open:
            emit_task_complete("dag_x", "task_y", "run_1",
                               ["https://api/ep"], ["s3://bucket/key.json"])
        request = mock_open.call_args[0][0]
        payload = json.loads(request.data.decode())
        assert payload["eventType"] == "COMPLETE"
        assert payload["job"]["name"] == "dag_x.task_y"

    def test_content_type_is_json(self):
        with self._fake_airflow_modules(), self._mock_urlopen() as mock_open:
            emit_task_complete("dag_x", "task_y", "run_1", [], [])
        request = mock_open.call_args[0][0]
        assert request.get_header("Content-type") == "application/json"

    def test_skips_when_url_not_configured(self):
        with self._fake_airflow_modules(url=None), \
             mock.patch("urllib.request.urlopen") as mock_open:
            emit_task_complete("dag_x", "task_y", "run_1", [], [])
        mock_open.assert_not_called()

    def test_skips_when_airflow_absent(self):
        import sys

        saved = {k: v for k, v in sys.modules.items() if k.startswith("airflow")}
        for key in saved:
            sys.modules[key] = None  # type: ignore[assignment]
        try:
            with mock.patch("urllib.request.urlopen") as mock_open:
                emit_task_complete("d", "t", "r", [], [])
            mock_open.assert_not_called()
        finally:
            for key in saved:
                sys.modules[key] = saved[key]

    def test_http_error_does_not_propagate(self):
        with self._fake_airflow_modules():
            with mock.patch("urllib.request.urlopen", side_effect=OSError("timeout")):
                emit_task_complete("d", "t", "r", [], [])  # must not raise

    def test_logs_success(self, caplog):
        import logging

        with self._fake_airflow_modules(), self._mock_urlopen():
            with caplog.at_level(logging.INFO, logger="comtrade.lineage"):
                emit_task_complete("dag_x", "task_y", "run_1",
                                   ["https://api/ep"], ["s3://b/k"])
        assert any("COMPLETE emitted" in r.message for r in caplog.records)

    def test_logs_error_on_failure(self, caplog):
        import logging

        with self._fake_airflow_modules():
            with mock.patch("urllib.request.urlopen", side_effect=Exception("boom")):
                with caplog.at_level(logging.ERROR, logger="comtrade.lineage"):
                    emit_task_complete("d", "t", "r", [], [])
        assert any("Failed to emit" in r.message for r in caplog.records)

    def test_trailing_slash_stripped_from_base_url(self):
        with self._fake_airflow_modules(url="http://marquez:5000/"), \
             self._mock_urlopen() as mock_open:
            emit_task_complete("d", "t", "r", [], [])
        request = mock_open.call_args[0][0]
        assert request.full_url == "http://marquez:5000/api/v1/lineage"
