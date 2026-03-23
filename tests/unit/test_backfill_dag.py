"""
Unit tests for dags/comtrade_backfill.py

Tests the pure-Python helpers (_call_api, validate_conf task, run_backfill task)
and structural properties of the DAG without a live Airflow scheduler or DB.

Strategy: import the DAG module and call .function on @task-decorated callables
(same as test_dag_factory.py).  The whole module is skipped when Airflow is
not installed so the base test run always passes.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest

# Skip this entire module if Airflow is not installed
pytest.importorskip("airflow", reason="apache-airflow not installed")

pytestmark = pytest.mark.unit

# ── Path setup ────────────────────────────────────────────────────────────────

_DAGS_DIR = Path(__file__).parents[2] / "dags"
_PLUGINS_DIR = Path(__file__).parents[2] / "plugins"
for _p in (_PLUGINS_DIR, _DAGS_DIR):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

# ── Module import (after Airflow confirmed available) ─────────────────────────

import comtrade_backfill as _mod  # noqa: E402


# ── Helpers ───────────────────────────────────────────────────────────────────


def _ctx(run_id: str = "manual__2024-01-01T00:00:00+00:00", conf: dict | None = None) -> dict:
    return {
        "run_id": run_id,
        "dag_run": MagicMock(conf=conf or {}),
    }


# ── _call_api dispatch ────────────────────────────────────────────────────────


class TestCallApiDispatch:
    def test_preview_calls_get_preview(self):
        conf = {
            "endpoint": "preview",
            "type_code": "C",
            "freq_code": "A",
            "cl_code": "HS",
            "reporter_code": "842",
            "partner_code": None,
            "cmd_code": None,
            "flow_code": None,
        }
        with patch("comtrade_backfill.client.get_preview", return_value={}) as m:
            _mod._call_api(conf, "2022")
        m.assert_called_once_with(
            typeCode="C",
            freqCode="A",
            clCode="HS",
            reporterCode="842",
            period="2022",
            partnerCode=None,
            cmdCode=None,
            flowCode=None,
        )

    def test_preview_tariffline_routes_correctly(self):
        conf = {
            "endpoint": "previewTariffline",
            "type_code": "C",
            "freq_code": "A",
            "cl_code": "HS",
            "reporter_code": None,
            "partner_code": None,
            "cmd_code": None,
            "flow_code": None,
        }
        with patch("comtrade_backfill.client.get_preview_tariffline", return_value={}) as m:
            _mod._call_api(conf, "2021")
        m.assert_called_once()
        assert m.call_args.kwargs["period"] == "2021"

    def test_getmbs_calls_get_mbs_with_period(self):
        with patch("comtrade_backfill.client.get_mbs", return_value={}) as m:
            _mod._call_api({"endpoint": "getMBS"}, "202301")
        m.assert_called_once_with(period="202301")

    def test_unsupported_endpoint_raises_value_error(self):
        with pytest.raises(ValueError, match="Unsupported endpoint"):
            _mod._call_api({"endpoint": "bogus"}, "2022")

    def test_period_forwarded_to_preview(self):
        conf = {
            "endpoint": "preview",
            "type_code": "C",
            "freq_code": "A",
            "cl_code": "HS",
        }
        with patch("comtrade_backfill.client.get_preview", return_value={}) as m:
            _mod._call_api(conf, "2019")
        assert m.call_args.kwargs["period"] == "2019"


# ── validate_conf task ────────────────────────────────────────────────────────


class TestValidateConf:
    def _run(self, conf: dict | None) -> dict:
        return _mod.validate_conf.function(**_ctx(conf=conf))

    def test_valid_conf_returns_normalised_dict(self):
        result = self._run({"endpoint": "preview", "periods": ["2021", "2022"]})
        assert result["endpoint"] == "preview"
        assert result["periods"] == ["2021", "2022"]

    def test_default_type_code_is_C(self):
        assert self._run({"endpoint": "preview", "periods": ["2021"]})["type_code"] == "C"

    def test_default_freq_code_is_A(self):
        assert self._run({"endpoint": "preview", "periods": ["2021"]})["freq_code"] == "A"

    def test_default_cl_code_is_HS(self):
        assert self._run({"endpoint": "preview", "periods": ["2021"]})["cl_code"] == "HS"

    def test_custom_type_code_preserved(self):
        result = self._run({"endpoint": "preview", "periods": ["2021"], "type_code": "S"})
        assert result["type_code"] == "S"

    def test_reporter_code_preserved(self):
        result = self._run({"endpoint": "preview", "periods": ["2021"], "reporter_code": "842"})
        assert result["reporter_code"] == "842"

    def test_reporter_code_none_when_absent(self):
        result = self._run({"endpoint": "preview", "periods": ["2021"]})
        assert result["reporter_code"] is None

    def test_missing_endpoint_raises(self):
        with pytest.raises(ValueError, match="endpoint"):
            self._run({"periods": ["2021"]})

    def test_missing_periods_raises(self):
        with pytest.raises(ValueError, match="periods"):
            self._run({"endpoint": "preview"})

    def test_empty_periods_list_raises(self):
        with pytest.raises(ValueError, match="periods"):
            self._run({"endpoint": "preview", "periods": []})

    def test_unsupported_endpoint_raises(self):
        with pytest.raises(ValueError, match="Unsupported endpoint"):
            self._run({"endpoint": "unknown", "periods": ["2021"]})

    def test_empty_conf_raises(self):
        with pytest.raises(ValueError, match="endpoint"):
            self._run({})

    def test_none_conf_treated_as_empty(self):
        with pytest.raises(ValueError, match="endpoint"):
            self._run(None)


# ── run_backfill task ─────────────────────────────────────────────────────────


def _run_backfill(params: dict, api_side_effect=None, api_return=None):
    """Invoke run_backfill.function with mocked I/O helpers."""
    api_mock = MagicMock()
    if api_side_effect:
        api_mock.side_effect = api_side_effect
    else:
        api_mock.return_value = api_return or {"data": [{"x": 1}]}

    with (
        patch("comtrade_backfill._call_api", api_mock),
        patch("comtrade_backfill.Variable.get", return_value="test-bucket"),
        patch("comtrade_backfill.write_json_to_s3"),
        patch("comtrade_backfill.build_s3_key", return_value="comtrade/preview/key.json"),
    ):
        result = _mod.run_backfill.function(params, **_ctx())

    return result, api_mock


class TestRunBackfill:
    _BASE_PARAMS = {
        "endpoint":  "preview",
        "type_code": "C",
        "freq_code": "A",
        "cl_code":   "HS",
    }

    def _params(self, periods: list) -> dict:
        return {**self._BASE_PARAMS, "periods": periods}

    def test_returns_summary_dict(self):
        result, _ = _run_backfill(self._params(["2021"]))
        assert "2021" in result
        assert result["2021"]["status"] == "success"

    def test_all_periods_processed(self):
        result, mock = _run_backfill(self._params(["2020", "2021", "2022"]))
        assert mock.call_count == 3
        assert set(result.keys()) == {"2020", "2021", "2022"}

    def test_success_result_contains_key(self):
        result, _ = _run_backfill(self._params(["2021"]))
        assert "key" in result["2021"]

    def test_partial_failure_raises_runtime_error(self):
        def _fail_2020(conf, period):
            if period == "2020":
                raise ConnectionError("API down")
            return {}

        with pytest.raises(RuntimeError, match="2020"):
            _run_backfill(self._params(["2020", "2021"]), api_side_effect=_fail_2020)

    def test_total_failure_raises_runtime_error(self):
        with pytest.raises(RuntimeError, match="errors"):
            _run_backfill(self._params(["2021"]), api_side_effect=RuntimeError("boom"))

    def test_api_called_once_per_period(self):
        params = self._params(["2020", "2021"])
        _, mock = _run_backfill(params)
        assert mock.call_count == 2

    def test_api_called_with_correct_conf_and_period(self):
        params = {**self._BASE_PARAMS, "periods": ["2022"], "reporter_code": "842"}
        _, mock = _run_backfill(params)
        mock.assert_called_once_with(params, "2022")

    def test_monthly_period_yields_correct_year_month(self):
        """Period '202203' should produce year='2022', month='03' in the S3 key."""
        captured: list = []

        def _capture(**kwargs):
            captured.append(kwargs)
            return "key.json"

        with (
            patch("comtrade_backfill._call_api", return_value={}),
            patch("comtrade_backfill.Variable.get", return_value="bucket"),
            patch("comtrade_backfill.write_json_to_s3"),
            patch("comtrade_backfill.build_s3_key", side_effect=_capture),
        ):
            _mod.run_backfill.function(
                {**self._BASE_PARAMS, "periods": ["202203"]}, **_ctx()
            )

        assert captured[0]["year"] == "2022"
        assert captured[0]["month"] == "03"

    def test_annual_period_defaults_month_to_01(self):
        """Period '2022' (length 4) should default month to '01'."""
        captured: list = []

        def _capture(**kwargs):
            captured.append(kwargs)
            return "key.json"

        with (
            patch("comtrade_backfill._call_api", return_value={}),
            patch("comtrade_backfill.Variable.get", return_value="bucket"),
            patch("comtrade_backfill.write_json_to_s3"),
            patch("comtrade_backfill.build_s3_key", side_effect=_capture),
        ):
            _mod.run_backfill.function(
                {**self._BASE_PARAMS, "periods": ["2022"]}, **_ctx()
            )

        assert captured[0]["month"] == "01"


# ── SUPPORTED_ENDPOINTS ───────────────────────────────────────────────────────


class TestSupportedEndpoints:
    def test_is_a_list(self):
        assert isinstance(_mod.SUPPORTED_ENDPOINTS, list)

    def test_contains_preview(self):
        assert "preview" in _mod.SUPPORTED_ENDPOINTS

    def test_contains_preview_tariffline(self):
        assert "previewTariffline" in _mod.SUPPORTED_ENDPOINTS

    def test_contains_getmbs(self):
        assert "getMBS" in _mod.SUPPORTED_ENDPOINTS

    def test_has_at_least_three_entries(self):
        assert len(_mod.SUPPORTED_ENDPOINTS) >= 3
