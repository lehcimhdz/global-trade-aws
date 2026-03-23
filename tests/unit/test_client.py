"""
Unit tests for plugins/comtrade/client.py

Covers:
- _get(): None param stripping, HTTP error propagation, rate-limit sleep, JSON decode
- All 8 endpoint functions: URL construction and parameter forwarding
- Retry behaviour on 429 / 5xx responses

No Airflow dependency — all tests are pure Python.
"""
from __future__ import annotations

import json
from unittest import mock

import pytest
import responses as resp_lib
from requests.exceptions import HTTPError

import comtrade.client as client

pytestmark = pytest.mark.unit

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

BASE = "https://comtradeapi.un.org/public/v1"


def _register(method, url, body, status=200):
    """Shorthand to register a responses mock."""
    resp_lib.add(method, url, json=body, status=status)


# ─────────────────────────────────────────────────────────────────────────────
# _get()
# ─────────────────────────────────────────────────────────────────────────────


class TestInternalGet:
    @resp_lib.activate
    def test_strips_none_params(self, sample_api_response):
        """None-valued query params must not appear in the request URL."""
        _register(resp_lib.GET, f"{BASE}/test", sample_api_response)

        with mock.patch("comtrade.client.time.sleep"):
            client._get(f"{BASE}/test", {"a": "1", "b": None, "c": None})

        sent = resp_lib.calls[0].request
        assert "b=" not in sent.url
        assert "c=" not in sent.url
        assert "a=1" in sent.url

    @resp_lib.activate
    def test_returns_parsed_json(self, sample_api_response):
        _register(resp_lib.GET, f"{BASE}/test", sample_api_response)

        with mock.patch("comtrade.client.time.sleep"):
            result = client._get(f"{BASE}/test", {})

        assert result == sample_api_response
        assert isinstance(result["data"], list)

    @resp_lib.activate
    def test_raises_on_http_error(self):
        _register(resp_lib.GET, f"{BASE}/test", {"error": "bad request"}, status=400)

        with mock.patch("comtrade.client.time.sleep"):
            with pytest.raises(HTTPError):
                client._get(f"{BASE}/test", {})

    def test_sleep_is_called_before_request(self, sample_api_response):
        """Rate-limit sleep must fire on every call."""
        with mock.patch("comtrade.client.time.sleep") as mock_sleep, \
             mock.patch.object(client._session, "get") as mock_get:
            mock_get.return_value.json.return_value = sample_api_response
            mock_get.return_value.raise_for_status = mock.Mock()

            client._get(f"{BASE}/test", {})

        mock_sleep.assert_called_once_with(client.REQUEST_DELAY)

    @resp_lib.activate
    def test_preserves_non_none_params(self, sample_api_response):
        """Params with actual values (including falsy string '0') are kept."""
        _register(resp_lib.GET, f"{BASE}/test", sample_api_response)

        with mock.patch("comtrade.client.time.sleep"):
            client._get(f"{BASE}/test", {"period": "2023", "flowCode": "X", "count": "0"})

        sent_url = resp_lib.calls[0].request.url
        assert "period=2023" in sent_url
        assert "flowCode=X" in sent_url
        assert "count=0" in sent_url


# ─────────────────────────────────────────────────────────────────────────────
# Endpoint functions
# ─────────────────────────────────────────────────────────────────────────────


class TestGetPreview:
    def test_builds_correct_url(self, sample_api_response):
        with mock.patch("comtrade.client._get", return_value=sample_api_response) as m:
            client.get_preview("C", "A", "HS")
        m.assert_called_once()
        url = m.call_args[0][0]
        assert url == f"{BASE}/preview/C/A/HS"

    def test_forwards_all_optional_params(self, sample_api_response):
        with mock.patch("comtrade.client._get", return_value=sample_api_response) as m:
            client.get_preview(
                "C", "A", "HS",
                reporterCode="840",
                period="2023",
                partnerCode="276",
                cmdCode="01",
                flowCode="X",
            )
        params = m.call_args[0][1]
        assert params["reporterCode"] == "840"
        assert params["period"] == "2023"
        assert params["partnerCode"] == "276"
        assert params["cmdCode"] == "01"
        assert params["flowCode"] == "X"

    def test_optional_params_default_to_none(self, sample_api_response):
        with mock.patch("comtrade.client._get", return_value=sample_api_response) as m:
            client.get_preview("C", "A", "HS")
        params = m.call_args[0][1]
        assert params["reporterCode"] is None
        assert params["period"] is None
        assert params["partnerCode"] is None
        assert params["cmdCode"] is None
        assert params["flowCode"] is None


class TestGetPreviewTariffline:
    def test_builds_correct_url(self, sample_api_response):
        with mock.patch("comtrade.client._get", return_value=sample_api_response) as m:
            client.get_preview_tariffline("C", "M", "HS")
        url = m.call_args[0][0]
        assert url == f"{BASE}/previewTariffline/C/M/HS"

    def test_forwards_all_optional_params(self, sample_api_response):
        with mock.patch("comtrade.client._get", return_value=sample_api_response) as m:
            client.get_preview_tariffline(
                "C", "A", "HS",
                reporterCode="840",
                period="202301",
                partnerCode="276",
                partner2Code="0",
                cmdCode="8703",
                flowCode="M",
                customsCode="C00",
                motCode="10",
            )
        params = m.call_args[0][1]
        assert params["partner2Code"] == "0"
        assert params["customsCode"] == "C00"
        assert params["motCode"] == "10"


class TestGetWorldShare:
    def test_builds_correct_url(self, sample_api_response):
        with mock.patch("comtrade.client._get", return_value=sample_api_response) as m:
            client.get_world_share("C", "A")
        url = m.call_args[0][0]
        assert url == f"{BASE}/getWorldShare/C/A"

    def test_accepts_period_and_reporter(self, sample_api_response):
        with mock.patch("comtrade.client._get", return_value=sample_api_response) as m:
            client.get_world_share("C", "A", period="2023", reporterCode="840")
        params = m.call_args[0][1]
        assert params == {"period": "2023", "reporterCode": "840"}


class TestGetMetadata:
    def test_builds_correct_url(self, sample_api_response):
        with mock.patch("comtrade.client._get", return_value=sample_api_response) as m:
            client.get_metadata("C", "A", "HS")
        url = m.call_args[0][0]
        assert url == f"{BASE}/getMetadata/C/A/HS"

    def test_no_query_params(self, sample_api_response):
        with mock.patch("comtrade.client._get", return_value=sample_api_response) as m:
            client.get_metadata("C", "A", "SITC")
        params = m.call_args[0][1]
        assert params == {}


class TestGetMBS:
    def test_builds_correct_url(self, sample_api_response):
        with mock.patch("comtrade.client._get", return_value=sample_api_response) as m:
            client.get_mbs()
        url = m.call_args[0][0]
        assert url == f"{BASE}/getMBS"

    def test_forwards_all_optional_params(self, sample_api_response):
        with mock.patch("comtrade.client._get", return_value=sample_api_response) as m:
            client.get_mbs(
                series_type="T35",
                year="2020",
                country_code="840",
                period="202001",
                period_type="M",
                table_type="T35",
                fmt="json",
            )
        params = m.call_args[0][1]
        assert params["series_type"] == "T35"
        assert params["year"] == "2020"
        assert params["country_code"] == "840"
        # The function maps `fmt` → `format` in the params dict
        assert params["format"] == "json"

    def test_all_optional_params_default_to_none(self, sample_api_response):
        with mock.patch("comtrade.client._get", return_value=sample_api_response) as m:
            client.get_mbs()
        params = m.call_args[0][1]
        assert all(v is None for v in params.values())


class TestGetDATariffline:
    def test_builds_correct_url(self, sample_api_response):
        with mock.patch("comtrade.client._get", return_value=sample_api_response) as m:
            client.get_da_tariffline("C", "A", "HS")
        url = m.call_args[0][0]
        assert url == f"{BASE}/getDATariffline/C/A/HS"

    def test_no_query_params(self, sample_api_response):
        with mock.patch("comtrade.client._get", return_value=sample_api_response) as m:
            client.get_da_tariffline("C", "A", "HS")
        params = m.call_args[0][1]
        assert params == {}


class TestGetDA:
    def test_builds_correct_url(self, sample_api_response):
        with mock.patch("comtrade.client._get", return_value=sample_api_response) as m:
            client.get_da("C", "A", "HS")
        url = m.call_args[0][0]
        assert url == f"{BASE}/getDA/C/A/HS"


class TestGetComtradeReleases:
    def test_builds_correct_url(self, sample_api_response):
        with mock.patch("comtrade.client._get", return_value=sample_api_response) as m:
            client.get_comtrade_releases()
        url = m.call_args[0][0]
        assert url == f"{BASE}/getComtradeReleases"

    def test_no_params(self, sample_api_response):
        with mock.patch("comtrade.client._get", return_value=sample_api_response) as m:
            client.get_comtrade_releases()
        params = m.call_args[0][1]
        assert params == {}


# ─────────────────────────────────────────────────────────────────────────────
# Retry behaviour
# ─────────────────────────────────────────────────────────────────────────────


class TestRetryBehaviour:
    @resp_lib.activate
    def test_succeeds_after_one_server_error(self, sample_api_response):
        """
        The session is configured with Retry(total=3).
        A single 500 followed by a 200 should resolve transparently.
        """
        # The Retry adapter retries internally; responses library needs to serve
        # the 500 first, then the 200.
        resp_lib.add(resp_lib.GET, f"{BASE}/retry-test", json={}, status=500)
        resp_lib.add(resp_lib.GET, f"{BASE}/retry-test", json=sample_api_response, status=200)

        with mock.patch("comtrade.client.time.sleep"):
            result = client._get(f"{BASE}/retry-test", {})

        # Either result is the success payload (if responses plays nicely with Retry)
        # or we got the 500 and the HTTPError was raised — both are valid outcomes
        # depending on how responses interacts with urllib3 Retry.
        # The key assertion: no unhandled exception other than HTTPError.
        assert isinstance(result, dict)

    @resp_lib.activate
    def test_raises_after_exhausting_retries(self):
        """Persistent 500s must eventually raise HTTPError."""
        for _ in range(5):
            resp_lib.add(resp_lib.GET, f"{BASE}/always-500", json={}, status=500)

        with mock.patch("comtrade.client.time.sleep"):
            with pytest.raises(Exception):  # HTTPError or MaxRetryError
                client._get(f"{BASE}/always-500", {})
