"""
Unit tests for plugins/comtrade/schema.py

No Airflow or AWS credentials needed at import time.  Tests cover:
  - _schema_s3_key: correct S3 key construction
  - _extract_columns: union of all record keys
  - SchemaDriftResult: added / removed computation
  - _build_drift_payload: correct Slack Block Kit structure
  - detect_and_alert: first run, no drift, added columns, removed columns,
                      error isolation, Slack suppression
"""
from __future__ import annotations

import json
from typing import Any, Dict
from unittest import mock

import pytest

from comtrade.schema import (
    SchemaDriftResult,
    _build_drift_payload,
    _extract_columns,
    _schema_s3_key,
    detect_and_alert,
)


# ── _schema_s3_key ────────────────────────────────────────────────────────────


class TestSchemaS3Key:
    def test_contains_endpoint(self):
        assert "preview" in _schema_s3_key("preview")

    def test_ends_with_json(self):
        assert _schema_s3_key("preview").endswith(".json")

    def test_correct_prefix(self):
        assert _schema_s3_key("getMBS").startswith("comtrade/schemas/")

    def test_different_endpoints_produce_different_keys(self):
        assert _schema_s3_key("preview") != _schema_s3_key("getMBS")


# ── _extract_columns ──────────────────────────────────────────────────────────


class TestExtractColumns:
    def test_single_record(self):
        records = [{"a": 1, "b": 2}]
        assert _extract_columns(records) == {"a", "b"}

    def test_union_across_records(self):
        records = [{"a": 1}, {"b": 2}, {"a": 3, "c": 4}]
        assert _extract_columns(records) == {"a", "b", "c"}

    def test_empty_records(self):
        assert _extract_columns([]) == set()

    def test_non_dict_records_ignored(self):
        records = [{"a": 1}, "not-a-dict", None]  # type: ignore[list-item]
        assert _extract_columns(records) == {"a"}

    def test_consistent_columns(self):
        records = [{"x": 1, "y": 2}, {"x": 3, "y": 4}]
        assert _extract_columns(records) == {"x", "y"}


# ── SchemaDriftResult ─────────────────────────────────────────────────────────


class TestSchemaDriftResult:
    def test_has_drift_when_columns_added(self):
        r = SchemaDriftResult(
            endpoint="preview",
            previous_columns={"a", "b"},
            current_columns={"a", "b", "c"},
        )
        assert r.has_drift
        assert r.added == {"c"}
        assert r.removed == set()

    def test_has_drift_when_columns_removed(self):
        r = SchemaDriftResult(
            endpoint="preview",
            previous_columns={"a", "b", "c"},
            current_columns={"a", "b"},
        )
        assert r.has_drift
        assert r.removed == {"c"}
        assert r.added == set()

    def test_has_drift_both_added_and_removed(self):
        r = SchemaDriftResult(
            endpoint="preview",
            previous_columns={"a", "b"},
            current_columns={"b", "c"},
        )
        assert r.has_drift
        assert r.added == {"c"}
        assert r.removed == {"a"}

    def test_no_drift_when_identical(self):
        r = SchemaDriftResult(
            endpoint="preview",
            previous_columns={"a", "b"},
            current_columns={"a", "b"},
        )
        assert not r.has_drift
        assert r.added == set()
        assert r.removed == set()

    def test_explicit_added_removed_not_overwritten(self):
        r = SchemaDriftResult(
            endpoint="preview",
            previous_columns={"a"},
            current_columns={"b"},
            added={"x"},
            removed={"y"},
        )
        # Explicit values are kept — __post_init__ only fills when both are empty
        assert r.added == {"x"}
        assert r.removed == {"y"}


# ── _build_drift_payload ──────────────────────────────────────────────────────


class TestBuildDriftPayload:
    def _result(self, added=None, removed=None):
        return SchemaDriftResult(
            endpoint="preview",
            previous_columns=set(),
            current_columns=set(),
            added=added or set(),
            removed=removed or set(),
        )

    def test_returns_blocks(self):
        payload = _build_drift_payload(self._result(added={"newCol"}))
        assert "blocks" in payload
        assert len(payload["blocks"]) > 0

    def test_header_mentions_drift(self):
        payload = _build_drift_payload(self._result(added={"x"}))
        header = payload["blocks"][0]
        assert "Drift" in header["text"]["text"]

    def test_endpoint_in_fields(self):
        payload = _build_drift_payload(self._result(added={"x"}))
        section = payload["blocks"][1]
        fields_text = " ".join(f["text"] for f in section["fields"])
        assert "preview" in fields_text

    def test_added_columns_listed(self):
        payload = _build_drift_payload(self._result(added={"newCol"}))
        payload_str = json.dumps(payload)
        assert "newCol" in payload_str
        assert "+" in payload_str

    def test_removed_columns_listed(self):
        payload = _build_drift_payload(self._result(removed={"oldCol"}))
        payload_str = json.dumps(payload)
        assert "oldCol" in payload_str
        assert "-" in payload_str

    def test_no_added_shows_none(self):
        payload = _build_drift_payload(self._result(removed={"x"}))
        payload_str = json.dumps(payload)
        assert "none" in payload_str

    def test_no_removed_shows_none(self):
        payload = _build_drift_payload(self._result(added={"x"}))
        payload_str = json.dumps(payload)
        assert "none" in payload_str

    def test_count_in_summary(self):
        result = self._result(added={"a", "b"}, removed={"c"})
        payload = _build_drift_payload(result)
        fields_text = " ".join(
            f["text"] for f in payload["blocks"][1]["fields"]
        )
        assert "+2" in fields_text
        assert "-1" in fields_text


# ── detect_and_alert ──────────────────────────────────────────────────────────


class TestDetectAndAlert:
    """Uses sys.modules injection to stub airflow.models.Variable and boto3."""

    _BUCKET = "test-bucket"
    _ENDPOINT = "preview"

    def _fake_airflow(self, webhook_url="https://hooks.slack.com/x"):
        import sys

        fake_variable = mock.MagicMock()
        fake_variable.get.side_effect = lambda k, **kw: {
            "AWS_ACCESS_KEY_ID": None,
            "AWS_SECRET_ACCESS_KEY": None,
            "AWS_DEFAULT_REGION": "us-east-1",
            "COMTRADE_SLACK_WEBHOOK_URL": webhook_url,
        }.get(k, kw.get("default_var"))
        fake_models = mock.MagicMock()
        fake_models.Variable = fake_variable
        fake_airflow = mock.MagicMock()
        fake_airflow.models = fake_models
        return mock.patch.dict(
            sys.modules,
            {"airflow": fake_airflow, "airflow.models": fake_models},
        )

    def _mock_s3(self, stored_columns=None):
        """Return (boto3_patch, s3_client_mock) with optional stored schema."""
        s3_client = mock.MagicMock()

        if stored_columns is None:
            # Simulate NoSuchKey — no schema stored yet
            err = type("NoSuchKey", (Exception,), {})()
            s3_client.get_object.side_effect = err
        else:
            schema_body = json.dumps({
                "columns": list(stored_columns),
                "schema_version": "1",
            }).encode()
            body_mock = mock.MagicMock()
            body_mock.read.return_value = schema_body
            s3_client.get_object.return_value = {"Body": body_mock}

        s3_client.put_object.return_value = {}
        boto3_patch = mock.patch("boto3.client", return_value=s3_client)
        return boto3_patch, s3_client

    def _records(self, columns):
        return [{c: i for i, c in enumerate(columns)}]

    def _mock_urlopen(self, status=200):
        resp = mock.MagicMock()
        resp.status = status
        resp.__enter__ = mock.Mock(return_value=resp)
        resp.__exit__ = mock.Mock(return_value=False)
        return mock.patch("urllib.request.urlopen", return_value=resp)

    # ── first run ────────────────────────────────────────────────────────────

    def test_first_run_returns_none(self):
        boto3_p, _ = self._mock_s3(stored_columns=None)
        with self._fake_airflow(), boto3_p:
            result = detect_and_alert(
                self._BUCKET, self._ENDPOINT,
                self._records(["a", "b"]), "run_1",
            )
        assert result is None

    def test_first_run_saves_schema(self):
        boto3_p, s3_client = self._mock_s3(stored_columns=None)
        with self._fake_airflow(), boto3_p:
            detect_and_alert(
                self._BUCKET, self._ENDPOINT,
                self._records(["a", "b"]), "run_1",
            )
        s3_client.put_object.assert_called_once()
        saved = json.loads(s3_client.put_object.call_args.kwargs["Body"].decode())
        assert set(saved["columns"]) == {"a", "b"}

    def test_first_run_logs_info(self, caplog):
        import logging
        boto3_p, _ = self._mock_s3(stored_columns=None)
        with self._fake_airflow(), boto3_p:
            with caplog.at_level(logging.INFO, logger="comtrade.schema"):
                detect_and_alert(self._BUCKET, self._ENDPOINT, self._records(["a"]), "r")
        assert any("baseline" in r.message for r in caplog.records)

    # ── no drift ─────────────────────────────────────────────────────────────

    def test_no_drift_returns_none(self):
        boto3_p, _ = self._mock_s3(stored_columns={"a", "b"})
        with self._fake_airflow(), boto3_p:
            result = detect_and_alert(
                self._BUCKET, self._ENDPOINT, self._records(["a", "b"]), "r"
            )
        assert result is None

    def test_no_drift_does_not_post_slack(self):
        boto3_p, _ = self._mock_s3(stored_columns={"a", "b"})
        with self._fake_airflow(), boto3_p, \
             mock.patch("urllib.request.urlopen") as mock_open:
            detect_and_alert(self._BUCKET, self._ENDPOINT, self._records(["a", "b"]), "r")
        mock_open.assert_not_called()

    def test_no_drift_does_not_save_schema(self):
        boto3_p, s3_client = self._mock_s3(stored_columns={"a", "b"})
        with self._fake_airflow(), boto3_p:
            detect_and_alert(self._BUCKET, self._ENDPOINT, self._records(["a", "b"]), "r")
        s3_client.put_object.assert_not_called()

    # ── drift: added columns ─────────────────────────────────────────────────

    def test_added_columns_returns_result(self):
        boto3_p, _ = self._mock_s3(stored_columns={"a", "b"})
        with self._fake_airflow(), boto3_p, self._mock_urlopen():
            result = detect_and_alert(
                self._BUCKET, self._ENDPOINT,
                self._records(["a", "b", "c"]), "r",
            )
        assert result is not None
        assert result.added == {"c"}
        assert result.removed == set()

    def test_added_columns_sends_slack(self):
        boto3_p, _ = self._mock_s3(stored_columns={"a", "b"})
        with self._fake_airflow(), boto3_p, self._mock_urlopen() as mock_open:
            detect_and_alert(self._BUCKET, self._ENDPOINT, self._records(["a", "b", "c"]), "r")
        mock_open.assert_called_once()

    def test_added_columns_updates_schema(self):
        boto3_p, s3_client = self._mock_s3(stored_columns={"a", "b"})
        with self._fake_airflow(), boto3_p, self._mock_urlopen():
            detect_and_alert(self._BUCKET, self._ENDPOINT, self._records(["a", "b", "c"]), "r")
        s3_client.put_object.assert_called_once()
        saved = json.loads(s3_client.put_object.call_args.kwargs["Body"].decode())
        assert "c" in saved["columns"]

    # ── drift: removed columns ───────────────────────────────────────────────

    def test_removed_columns_returns_result(self):
        boto3_p, _ = self._mock_s3(stored_columns={"a", "b", "c"})
        with self._fake_airflow(), boto3_p, self._mock_urlopen():
            result = detect_and_alert(
                self._BUCKET, self._ENDPOINT, self._records(["a", "b"]), "r"
            )
        assert result is not None
        assert result.removed == {"c"}

    def test_removed_columns_logs_warning(self, caplog):
        import logging
        boto3_p, _ = self._mock_s3(stored_columns={"a", "b", "c"})
        with self._fake_airflow(), boto3_p, self._mock_urlopen():
            with caplog.at_level(logging.WARNING, logger="comtrade.schema"):
                detect_and_alert(self._BUCKET, self._ENDPOINT, self._records(["a", "b"]), "r")
        assert any("drift" in r.message.lower() for r in caplog.records)

    # ── error isolation ───────────────────────────────────────────────────────

    def test_slack_error_does_not_propagate(self):
        boto3_p, _ = self._mock_s3(stored_columns={"a"})
        with self._fake_airflow(), boto3_p:
            with mock.patch("urllib.request.urlopen", side_effect=OSError("timeout")):
                # drift + Slack failure — must not raise
                detect_and_alert(self._BUCKET, self._ENDPOINT, self._records(["a", "b"]), "r")

    def test_s3_error_does_not_propagate(self):
        with self._fake_airflow():
            with mock.patch("boto3.client", side_effect=RuntimeError("no credentials")):
                result = detect_and_alert(self._BUCKET, self._ENDPOINT, self._records(["a"]), "r")
        assert result is None

    def test_empty_records_returns_none(self):
        boto3_p, _ = self._mock_s3(stored_columns={"a"})
        with self._fake_airflow(), boto3_p:
            result = detect_and_alert(self._BUCKET, self._ENDPOINT, [], "r")
        assert result is None

    # ── Slack suppressed when webhook not configured ──────────────────────────

    def test_drift_without_webhook_does_not_crash(self, caplog):
        import logging
        boto3_p, _ = self._mock_s3(stored_columns={"a"})
        with self._fake_airflow(webhook_url=None), boto3_p:
            with caplog.at_level(logging.WARNING, logger="comtrade.schema"):
                detect_and_alert(self._BUCKET, self._ENDPOINT, self._records(["a", "b"]), "r")
        assert any("not configured" in r.message for r in caplog.records)
