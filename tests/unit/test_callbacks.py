"""
Unit tests for plugins/comtrade/callbacks.py

The module has no Airflow dependency at import time — all Airflow references
are lazy (inside functions).  These tests cover:
  - Dead-letter manifest structure and S3 key format
  - Slack payload structure
  - Behaviour when webhook URL is missing
  - HTTP posting (mocked)
  - Error isolation (notification failures must not propagate)
"""
from __future__ import annotations

import json
from typing import Any, Dict
from unittest import mock

import pytest

from comtrade.callbacks import (
    _build_error_manifest,
    _build_sla_miss_payload,
    _build_task_failure_payload,
    _get_webhook_url,
    _post_slack,
    sla_miss_callback,
    task_failure_callback,
    write_error_manifest,
)


# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_context(
    dag_id: str = "comtrade_preview",
    task_id: str = "extract_and_store_raw",
    run_id: str = "scheduled__2024-01-01T00:00:00+00:00",
    execution_date: str = "2024-01-01T00:00:00+00:00",
    log_url: str = "http://airflow:8080/log",
    exception: Exception | None = None,
) -> Dict[str, Any]:
    dag = mock.Mock()
    dag.dag_id = dag_id

    ti = mock.Mock()
    ti.task_id = task_id
    ti.log_url = log_url

    return {
        "dag": dag,
        "task_instance": ti,
        "run_id": run_id,
        "execution_date": execution_date,
        "exception": exception,
    }


# ── _build_error_manifest ─────────────────────────────────────────────────────


class TestBuildErrorManifest:
    def test_contains_required_fields(self):
        ctx = _make_context()
        manifest = _build_error_manifest(ctx)
        for field in ("schema_version", "dag_id", "task_id", "run_id",
                      "execution_date", "failed_at", "exception_type", "exception_message"):
            assert field in manifest, f"missing field: {field}"

    def test_schema_version_is_string_one(self):
        manifest = _build_error_manifest(_make_context())
        assert manifest["schema_version"] == "1"

    def test_dag_id_captured(self):
        ctx = _make_context(dag_id="comtrade_mbs")
        assert _build_error_manifest(ctx)["dag_id"] == "comtrade_mbs"

    def test_task_id_captured(self):
        ctx = _make_context(task_id="validate_bronze")
        assert _build_error_manifest(ctx)["task_id"] == "validate_bronze"

    def test_run_id_captured(self):
        ctx = _make_context(run_id="manual__2024-06-01")
        assert _build_error_manifest(ctx)["run_id"] == "manual__2024-06-01"

    def test_exception_type_captured(self):
        ctx = _make_context(exception=ValueError("bad"))
        assert _build_error_manifest(ctx)["exception_type"] == "ValueError"

    def test_exception_message_captured(self):
        ctx = _make_context(exception=RuntimeError("boom"))
        assert _build_error_manifest(ctx)["exception_message"] == "boom"

    def test_no_exception_fields_are_none(self):
        ctx = _make_context(exception=None)
        manifest = _build_error_manifest(ctx)
        assert manifest["exception_type"] is None
        assert manifest["exception_message"] is None

    def test_execution_date_as_string(self):
        ctx = _make_context(execution_date="2024-03-01T00:00:00+00:00")
        manifest = _build_error_manifest(ctx)
        assert "2024-03-01" in manifest["execution_date"]

    def test_failed_at_is_iso_format(self):
        import re
        manifest = _build_error_manifest(_make_context())
        # ISO 8601: YYYY-MM-DDTHH:MM:SS...
        assert re.match(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", manifest["failed_at"])


# ── write_error_manifest ──────────────────────────────────────────────────────


class TestWriteErrorManifest:
    def _fake_variable_get(self, key, default_var=None, **kwargs):
        return {
            "COMTRADE_S3_BUCKET": "test-bucket",
            "AWS_ACCESS_KEY_ID": None,
            "AWS_SECRET_ACCESS_KEY": None,
            "AWS_DEFAULT_REGION": "us-east-1",
        }.get(key, default_var)

    def _fake_airflow_modules(self):
        """Return a sys.modules patch that provides a stub airflow.models.Variable."""
        import sys

        fake_variable = mock.MagicMock()
        fake_variable.get.side_effect = self._fake_variable_get

        fake_models = mock.MagicMock()
        fake_models.Variable = fake_variable

        fake_airflow = mock.MagicMock()
        fake_airflow.models = fake_models

        return mock.patch.dict(
            sys.modules,
            {"airflow": fake_airflow, "airflow.models": fake_models},
        )

    def _mock_boto(self):
        s3 = mock.MagicMock()
        client = mock.MagicMock()
        client.put_object.return_value = {}
        s3.return_value = client
        return s3, client

    def test_returns_s3_key_on_success(self):
        s3_mock, _ = self._mock_boto()
        ctx = _make_context(dag_id="comtrade_preview", task_id="validate_bronze",
                            run_id="scheduled__2024-03-01")
        fake_date = mock.MagicMock()
        fake_date.strftime.side_effect = lambda fmt: {"%-m": "3", "%Y": "2024", "%m": "03"}[fmt] if fmt in ("%Y", "%m") else "2024"
        ctx["execution_date"] = fake_date

        with mock.patch("boto3.client", s3_mock), self._fake_airflow_modules():
            result = write_error_manifest(ctx)
        assert result is not None
        assert result.startswith("comtrade/errors/")
        assert result.endswith(".json")

    def test_s3_key_contains_dag_and_task(self):
        s3_mock, _ = self._mock_boto()
        ctx = _make_context(dag_id="comtrade_da", task_id="extract_and_store_raw",
                            run_id="scheduled__2024-01-01")
        with mock.patch("boto3.client", s3_mock), self._fake_airflow_modules():
            key = write_error_manifest(ctx)
        assert key is not None
        assert "dag_id=comtrade_da" in key
        assert "task_id=extract_and_store_raw" in key

    def test_s3_key_is_hive_partitioned(self):
        s3_mock, _ = self._mock_boto()
        ctx = _make_context(run_id="scheduled__2024-06-15")
        with mock.patch("boto3.client", s3_mock), self._fake_airflow_modules():
            key = write_error_manifest(ctx)
        assert key is not None
        assert "/year=" in key
        assert "/month=" in key

    def test_returns_none_when_boto_fails(self):
        ctx = _make_context()
        with mock.patch("boto3.client", side_effect=RuntimeError("no credentials")):
            with self._fake_airflow_modules():
                result = write_error_manifest(ctx)
        assert result is None

    def test_returns_none_when_airflow_absent(self):
        """If Airflow is not installed the function catches the ImportError and returns None."""
        import sys
        saved = {k: v for k, v in sys.modules.items() if k.startswith("airflow")}
        for key in saved:
            sys.modules[key] = None  # type: ignore[assignment]
        try:
            result = write_error_manifest(_make_context())
            assert result is None
        finally:
            for key in saved:
                sys.modules[key] = saved[key]

    def test_put_object_called_with_correct_bucket(self):
        s3_mock, s3_client = self._mock_boto()
        ctx = _make_context()
        with mock.patch("boto3.client", s3_mock), self._fake_airflow_modules():
            write_error_manifest(ctx)
        call_kwargs = s3_client.put_object.call_args
        assert call_kwargs is not None
        assert call_kwargs.kwargs.get("Bucket") == "test-bucket"

    def test_put_object_body_is_valid_json(self):
        s3_mock, s3_client = self._mock_boto()
        ctx = _make_context(exception=ValueError("bad column"))
        with mock.patch("boto3.client", s3_mock), self._fake_airflow_modules():
            write_error_manifest(ctx)
        call_kwargs = s3_client.put_object.call_args
        assert call_kwargs is not None
        body = call_kwargs.kwargs.get("Body", b"")
        data = json.loads(body.decode())
        assert data["exception_type"] == "ValueError"
        assert data["exception_message"] == "bad column"


# ── _build_task_failure_payload ───────────────────────────────────────────────


class TestBuildTaskFailurePayload:
    def test_returns_dict_with_blocks(self):
        ctx = _make_context()
        payload = _build_task_failure_payload(ctx)
        assert "blocks" in payload
        assert isinstance(payload["blocks"], list)
        assert len(payload["blocks"]) > 0

    def test_header_block_present(self):
        ctx = _make_context()
        payload = _build_task_failure_payload(ctx)
        header = payload["blocks"][0]
        assert header["type"] == "header"
        assert "Failed" in header["text"]["text"]

    def test_dag_id_in_fields(self):
        ctx = _make_context(dag_id="comtrade_mbs")
        payload = _build_task_failure_payload(ctx)
        section = payload["blocks"][1]
        fields_text = " ".join(f["text"] for f in section["fields"])
        assert "comtrade_mbs" in fields_text

    def test_task_id_in_fields(self):
        ctx = _make_context(task_id="validate_bronze")
        payload = _build_task_failure_payload(ctx)
        section = payload["blocks"][1]
        fields_text = " ".join(f["text"] for f in section["fields"])
        assert "validate_bronze" in fields_text

    def test_exception_message_in_payload(self):
        ctx = _make_context(exception=ValueError("bad data"))
        payload = _build_task_failure_payload(ctx)
        error_section = payload["blocks"][2]
        assert "bad data" in error_section["text"]["text"]

    def test_long_exception_truncated(self):
        ctx = _make_context(exception=Exception("x" * 1000))
        payload = _build_task_failure_payload(ctx)
        error_section = payload["blocks"][2]
        # The raw exception string (1000 chars) must be trimmed
        assert len(error_section["text"]["text"]) < 1000

    def test_log_url_in_actions_block(self):
        ctx = _make_context(log_url="http://airflow:8080/task-log")
        payload = _build_task_failure_payload(ctx)
        actions_block = payload["blocks"][3]
        assert actions_block["type"] == "actions"
        button_url = actions_block["elements"][0]["url"]
        assert "task-log" in button_url

    def test_no_exception_falls_back_to_default_text(self):
        ctx = _make_context(exception=None)
        payload = _build_task_failure_payload(ctx)
        error_section = payload["blocks"][2]
        assert "No exception captured" in error_section["text"]["text"]

    def test_run_id_in_fields(self):
        ctx = _make_context(run_id="manual__2024-06-01")
        payload = _build_task_failure_payload(ctx)
        section = payload["blocks"][1]
        fields_text = " ".join(f["text"] for f in section["fields"])
        assert "manual__2024-06-01" in fields_text


# ── _get_webhook_url ──────────────────────────────────────────────────────────


class TestGetWebhookUrl:
    def test_returns_none_when_airflow_not_importable(self):
        """If Airflow raises on import the function catches it and returns None."""
        import sys

        # Temporarily hide airflow from the import system
        saved = {k: v for k, v in sys.modules.items() if k.startswith("airflow")}
        for key in saved:
            sys.modules[key] = None  # type: ignore[assignment]
        try:
            result = _get_webhook_url()
            assert result is None
        finally:
            for key in saved:
                sys.modules[key] = saved[key]

    def test_returns_url_when_variable_available(self):
        """When Variable.get succeeds the URL is forwarded."""
        fake_variable = mock.MagicMock()
        fake_variable.get.return_value = "https://hooks.slack.com/abc"
        fake_airflow = mock.MagicMock()
        fake_airflow.Variable = fake_variable

        import sys

        with mock.patch.dict(sys.modules, {"airflow.models": fake_airflow}):
            result = _get_webhook_url()

        assert result == "https://hooks.slack.com/abc"


# ── _post_slack ───────────────────────────────────────────────────────────────


class TestPostSlack:
    def _mock_response(self, status=200):
        resp = mock.MagicMock()
        resp.status = status
        resp.__enter__ = mock.Mock(return_value=resp)
        resp.__exit__ = mock.Mock(return_value=False)
        return resp

    def test_posts_json_content_type(self):
        resp = self._mock_response()
        with mock.patch("urllib.request.urlopen", return_value=resp) as mock_open:
            _post_slack({"blocks": []}, "https://hooks.slack.com/test")
            request = mock_open.call_args[0][0]
            assert request.get_header("Content-type") == "application/json"

    def test_posts_correct_url(self):
        resp = self._mock_response()
        webhook = "https://hooks.slack.com/test-url"
        with mock.patch("urllib.request.urlopen", return_value=resp) as mock_open:
            _post_slack({"blocks": []}, webhook)
            request = mock_open.call_args[0][0]
            assert request.full_url == webhook

    def test_payload_serialized_as_json(self):
        resp = self._mock_response()
        payload = {"blocks": [{"type": "header"}]}
        with mock.patch("urllib.request.urlopen", return_value=resp) as mock_open:
            _post_slack(payload, "https://hooks.slack.com/test")
            request = mock_open.call_args[0][0]
            sent_data = json.loads(request.data.decode())
            assert sent_data == payload

    def test_non_200_logs_error(self, caplog):
        import logging
        resp = self._mock_response(status=500)
        resp.read.return_value = b"server error"
        with mock.patch("urllib.request.urlopen", return_value=resp):
            with caplog.at_level(logging.ERROR, logger="comtrade.callbacks"):
                _post_slack({"blocks": []}, "https://hooks.slack.com/test")
        assert any("500" in r.message for r in caplog.records)


# ── _build_sla_miss_payload ───────────────────────────────────────────────────


class TestBuildSlaMissPayload:
    def _make_dag(self, dag_id="comtrade_preview"):
        dag = mock.Mock()
        dag.dag_id = dag_id
        return dag

    def _make_sla(self, execution_date="2024-01-01"):
        sla = mock.Mock()
        sla.execution_date = execution_date
        return sla

    def test_returns_dict_with_blocks(self):
        payload = _build_sla_miss_payload(self._make_dag(), ["task_a"], [], [], [])
        assert "blocks" in payload
        assert isinstance(payload["blocks"], list)

    def test_header_contains_sla_miss_text(self):
        payload = _build_sla_miss_payload(self._make_dag(), [], [], [], [])
        header = payload["blocks"][0]
        assert "SLA" in header["text"]["text"]

    def test_dag_id_in_fields(self):
        payload = _build_sla_miss_payload(self._make_dag("comtrade_mbs"), [], [], [], [])
        section = payload["blocks"][1]
        fields_text = " ".join(f["text"] for f in section["fields"])
        assert "comtrade_mbs" in fields_text

    def test_missed_tasks_in_fields(self):
        payload = _build_sla_miss_payload(
            self._make_dag(), ["extract_and_store_raw", "validate_bronze"], [], [], []
        )
        section = payload["blocks"][1]
        fields_text = " ".join(f["text"] for f in section["fields"])
        assert "extract_and_store_raw" in fields_text
        assert "validate_bronze" in fields_text

    def test_blocking_tasks_in_fields(self):
        payload = _build_sla_miss_payload(
            self._make_dag(), [], ["convert_to_parquet"], [], []
        )
        section = payload["blocks"][1]
        fields_text = " ".join(f["text"] for f in section["fields"])
        assert "convert_to_parquet" in fields_text

    def test_execution_date_extracted_from_slas(self):
        sla = self._make_sla("2024-06-01")
        payload = _build_sla_miss_payload(self._make_dag(), [], [], [sla], [])
        section = payload["blocks"][1]
        fields_text = " ".join(f["text"] for f in section["fields"])
        assert "2024-06-01" in fields_text

    def test_empty_task_list_shows_unknown(self):
        payload = _build_sla_miss_payload(self._make_dag(), [], [], [], [])
        section = payload["blocks"][1]
        fields_text = " ".join(f["text"] for f in section["fields"])
        assert "unknown" in fields_text

    def test_empty_blocking_list_shows_none(self):
        payload = _build_sla_miss_payload(self._make_dag(), [], [], [], [])
        section = payload["blocks"][1]
        fields_text = " ".join(f["text"] for f in section["fields"])
        assert "none" in fields_text

    def test_slas_without_execution_date_attr_handled(self):
        sla = mock.Mock(spec=[])  # no attributes
        payload = _build_sla_miss_payload(self._make_dag(), [], [], [sla], [])
        section = payload["blocks"][1]
        fields_text = " ".join(f["text"] for f in section["fields"])
        assert "unknown" in fields_text


# ── sla_miss_callback ─────────────────────────────────────────────────────────


class TestSlaMissCallback:
    def _make_dag(self, dag_id="comtrade_preview"):
        dag = mock.Mock()
        dag.dag_id = dag_id
        return dag

    def test_skips_when_no_webhook_url(self, caplog):
        import logging

        with mock.patch("comtrade.callbacks._get_webhook_url", return_value=None):
            with caplog.at_level(logging.WARNING, logger="comtrade.callbacks"):
                sla_miss_callback(self._make_dag(), [], [], [], [])
        assert any("not configured" in r.message for r in caplog.records)

    def test_posts_when_webhook_configured(self):
        with mock.patch("comtrade.callbacks._get_webhook_url", return_value="https://hooks.slack.com/x"):
            with mock.patch("comtrade.callbacks._post_slack") as mock_post:
                sla_miss_callback(self._make_dag(), ["extract_and_store_raw"], [], [], [])
        mock_post.assert_called_once()
        payload, url = mock_post.call_args[0]
        assert "blocks" in payload
        assert url == "https://hooks.slack.com/x"

    def test_notification_error_does_not_propagate(self):
        with mock.patch("comtrade.callbacks._get_webhook_url", return_value="https://hooks.slack.com/x"):
            with mock.patch("comtrade.callbacks._post_slack", side_effect=RuntimeError("timeout")):
                sla_miss_callback(self._make_dag(), [], [], [], [])  # must not raise

    def test_dag_id_in_slack_payload(self):
        with mock.patch("comtrade.callbacks._get_webhook_url", return_value="https://hooks.slack.com/x"):
            with mock.patch("comtrade.callbacks._post_slack") as mock_post:
                sla_miss_callback(self._make_dag("comtrade_releases"), [], [], [], [])
        payload = mock_post.call_args[0][0]
        assert "comtrade_releases" in json.dumps(payload)

    def test_logs_success_after_post(self, caplog):
        import logging

        with mock.patch("comtrade.callbacks._get_webhook_url", return_value="https://hooks.slack.com/x"):
            with mock.patch("comtrade.callbacks._post_slack"):
                with caplog.at_level(logging.INFO, logger="comtrade.callbacks"):
                    sla_miss_callback(self._make_dag(), [], [], [], [])
        assert any("SLA miss alert sent" in r.message for r in caplog.records)


# ── task_failure_callback ─────────────────────────────────────────────────────


class TestTaskFailureCallback:
    def test_always_calls_write_error_manifest(self):
        """Dead-letter write happens regardless of Slack config."""
        ctx = _make_context()
        with mock.patch("comtrade.callbacks.write_error_manifest") as mock_manifest:
            with mock.patch("comtrade.callbacks._get_webhook_url", return_value=None):
                task_failure_callback(ctx)
        mock_manifest.assert_called_once_with(ctx)

    def test_write_manifest_called_even_when_slack_configured(self):
        ctx = _make_context()
        with mock.patch("comtrade.callbacks.write_error_manifest") as mock_manifest:
            with mock.patch("comtrade.callbacks._get_webhook_url", return_value="https://hooks.slack.com/x"):
                with mock.patch("comtrade.callbacks._post_slack"):
                    task_failure_callback(ctx)
        mock_manifest.assert_called_once_with(ctx)

    def test_skips_when_no_webhook_url(self, caplog):
        import logging

        ctx = _make_context()
        with mock.patch("comtrade.callbacks.write_error_manifest"):
            with mock.patch("comtrade.callbacks._get_webhook_url", return_value=None):
                with caplog.at_level(logging.WARNING, logger="comtrade.callbacks"):
                    task_failure_callback(ctx)  # must not raise
        assert any("not configured" in r.message for r in caplog.records)

    def test_posts_when_webhook_configured(self):
        ctx = _make_context()
        with mock.patch("comtrade.callbacks.write_error_manifest"):
            with mock.patch("comtrade.callbacks._get_webhook_url", return_value="https://hooks.slack.com/x"):
                with mock.patch("comtrade.callbacks._post_slack") as mock_post:
                    task_failure_callback(ctx)
        mock_post.assert_called_once()
        payload, url = mock_post.call_args[0]
        assert "blocks" in payload
        assert url == "https://hooks.slack.com/x"

    def test_notification_error_does_not_propagate(self):
        ctx = _make_context()
        with mock.patch("comtrade.callbacks.write_error_manifest"):
            with mock.patch("comtrade.callbacks._get_webhook_url", return_value="https://hooks.slack.com/x"):
                with mock.patch("comtrade.callbacks._post_slack", side_effect=RuntimeError("network down")):
                    task_failure_callback(ctx)  # must not raise

    def test_logs_success_after_post(self, caplog):
        import logging

        ctx = _make_context()
        with mock.patch("comtrade.callbacks.write_error_manifest"):
            with mock.patch("comtrade.callbacks._get_webhook_url", return_value="https://hooks.slack.com/x"):
                with mock.patch("comtrade.callbacks._post_slack"):
                    with caplog.at_level(logging.INFO, logger="comtrade.callbacks"):
                        task_failure_callback(ctx)
        assert any("alert sent" in r.message for r in caplog.records)

    def test_dag_id_and_task_id_in_slack_payload(self):
        ctx = _make_context(dag_id="comtrade_releases", task_id="convert_to_parquet")
        with mock.patch("comtrade.callbacks.write_error_manifest"):
            with mock.patch("comtrade.callbacks._get_webhook_url", return_value="https://hooks.slack.com/x"):
                with mock.patch("comtrade.callbacks._post_slack") as mock_post:
                    task_failure_callback(ctx)
        payload = mock_post.call_args[0][0]
        payload_str = json.dumps(payload)
        assert "comtrade_releases" in payload_str
        assert "convert_to_parquet" in payload_str
