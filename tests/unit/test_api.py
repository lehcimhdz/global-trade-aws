"""
Unit tests for the trade API (api/main.py and api/athena.py).

All Athena calls are mocked — no AWS credentials or real infrastructure needed.
"""
from __future__ import annotations

import time
from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest

# ── Availability guard ────────────────────────────────────────────────────────
# FastAPI and httpx (used by TestClient) may not be installed in the base env.

try:
    from fastapi.testclient import TestClient

    from api.main import app
    from api.athena import AthenaQueryError, run_query

    _AVAILABLE = True
except ImportError:
    _AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not _AVAILABLE, reason="fastapi / httpx not installed"
)


# ── Shared fixtures & helpers ─────────────────────────────────────────────────


@pytest.fixture()
def client():
    with TestClient(app, raise_server_exceptions=True) as c:
        yield c


REPORTER_ROW: dict[str, Any] = {
    "reporter_iso": "USA",
    "reporter_name": "United States",
    "period": "2022",
    "export_value_usd": "2000000000000",
    "import_value_usd": "3000000000000",
    "total_trade_value_usd": "5000000000000",
    "trade_balance_usd": "-1000000000000",
    "commodity_count": "5000",
    "partner_count": "200",
}

FLOW_ROW: dict[str, Any] = {
    "period": "2022",
    "reporter_iso": "USA",
    "reporter_name": "United States",
    "partner_iso": "CHN",
    "partner_name": "China",
    "commodity_code": "85",
    "commodity_name": "Electrical machinery",
    "flow_code": "M",
    "flow_name": "Import",
    "trade_value_usd": "500000000000",
}


def _mock_run(rows: list[dict]) -> MagicMock:
    """Return a mock for api.athena.run_query that returns *rows*."""
    m = MagicMock(return_value=rows)
    return m


# ── Health ─────────────────────────────────────────────────────────────────────


class TestHealthEndpoint:
    def test_returns_200(self, client):
        r = client.get("/health")
        assert r.status_code == 200

    def test_body_contains_status_ok(self, client):
        r = client.get("/health")
        assert r.json() == {"status": "ok"}


# ── GET /v1/reporters ─────────────────────────────────────────────────────────


class TestListReporters:
    def test_returns_200_with_data(self, client):
        with patch("api.main.run_query", return_value=[REPORTER_ROW]):
            r = client.get("/v1/reporters")
        assert r.status_code == 200
        assert r.json()["data"] == [REPORTER_ROW]

    def test_default_limit_in_sql(self, client):
        mock = _mock_run([REPORTER_ROW])
        with patch("api.main.run_query", mock):
            client.get("/v1/reporters")
        sql: str = mock.call_args[0][0]
        assert "LIMIT 20" in sql

    def test_custom_limit_in_sql(self, client):
        mock = _mock_run([REPORTER_ROW])
        with patch("api.main.run_query", mock):
            client.get("/v1/reporters?limit=5")
        sql: str = mock.call_args[0][0]
        assert "LIMIT 5" in sql

    def test_period_filter_in_sql(self, client):
        mock = _mock_run([REPORTER_ROW])
        with patch("api.main.run_query", mock):
            client.get("/v1/reporters?period=2022")
        sql: str = mock.call_args[0][0]
        assert "period = '2022'" in sql

    def test_freq_code_always_filtered_to_annual(self, client):
        mock = _mock_run([REPORTER_ROW])
        with patch("api.main.run_query", mock):
            client.get("/v1/reporters")
        sql: str = mock.call_args[0][0]
        assert "freq_code = 'A'" in sql

    def test_orders_by_total_trade_value(self, client):
        mock = _mock_run([REPORTER_ROW])
        with patch("api.main.run_query", mock):
            client.get("/v1/reporters")
        sql: str = mock.call_args[0][0]
        assert "total_trade_value_usd DESC" in sql

    def test_empty_result_returns_empty_list(self, client):
        with patch("api.main.run_query", return_value=[]):
            r = client.get("/v1/reporters")
        assert r.status_code == 200
        assert r.json()["data"] == []

    def test_invalid_period_returns_422(self, client):
        r = client.get("/v1/reporters?period=20xx")
        assert r.status_code == 422

    def test_limit_above_max_returns_422(self, client):
        r = client.get("/v1/reporters?limit=999")
        assert r.status_code == 422

    def test_limit_zero_returns_422(self, client):
        r = client.get("/v1/reporters?limit=0")
        assert r.status_code == 422

    def test_athena_error_returns_500(self, client):
        with patch("api.main.run_query", side_effect=AthenaQueryError("boom")):
            r = client.get("/v1/reporters")
        assert r.status_code == 500
        assert "boom" in r.json()["detail"]


# ── GET /v1/reporters/{reporter_iso}/summary ──────────────────────────────────


SUMMARY_ROW: dict[str, Any] = {
    "period": "2022",
    "export_value_usd": "2000000000000",
    "import_value_usd": "3000000000000",
    "total_trade_value_usd": "5000000000000",
    "trade_balance_usd": "-1000000000000",
    "commodity_count": "5000",
    "partner_count": "200",
}


class TestReporterSummary:
    def test_returns_200_with_rows(self, client):
        with patch("api.main.run_query", return_value=[SUMMARY_ROW]):
            r = client.get("/v1/reporters/USA/summary")
        assert r.status_code == 200
        body = r.json()
        assert body["reporter_iso"] == "USA"
        assert body["data"] == [SUMMARY_ROW]

    def test_iso_normalised_to_uppercase(self, client):
        mock = _mock_run([SUMMARY_ROW])
        with patch("api.main.run_query", mock):
            client.get("/v1/reporters/usa/summary")
        sql: str = mock.call_args[0][0]
        assert "reporter_iso = 'USA'" in sql

    def test_orders_by_period_desc(self, client):
        mock = _mock_run([SUMMARY_ROW])
        with patch("api.main.run_query", mock):
            client.get("/v1/reporters/USA/summary")
        sql: str = mock.call_args[0][0]
        assert "ORDER BY period DESC" in sql

    def test_custom_limit_in_sql(self, client):
        mock = _mock_run([SUMMARY_ROW])
        with patch("api.main.run_query", mock):
            client.get("/v1/reporters/USA/summary?limit=3")
        sql: str = mock.call_args[0][0]
        assert "LIMIT 3" in sql

    def test_unknown_reporter_returns_404(self, client):
        with patch("api.main.run_query", return_value=[]):
            r = client.get("/v1/reporters/ZZZ/summary")
        assert r.status_code == 404

    def test_invalid_iso_path_returns_422(self, client):
        r = client.get("/v1/reporters/US123/summary")
        assert r.status_code == 422

    def test_athena_error_returns_500(self, client):
        with patch("api.main.run_query", side_effect=AthenaQueryError("athena down")):
            r = client.get("/v1/reporters/USA/summary")
        assert r.status_code == 500


# ── GET /v1/trade-flows ───────────────────────────────────────────────────────


class TestTradeFlows:
    def test_returns_200_with_data(self, client):
        with patch("api.main.run_query", return_value=[FLOW_ROW]):
            r = client.get("/v1/trade-flows?reporter_iso=USA")
        assert r.status_code == 200
        assert r.json()["data"] == [FLOW_ROW]

    def test_reporter_iso_filter_in_sql(self, client):
        mock = _mock_run([FLOW_ROW])
        with patch("api.main.run_query", mock):
            client.get("/v1/trade-flows?reporter_iso=USA")
        sql: str = mock.call_args[0][0]
        assert "reporter_iso = 'USA'" in sql

    def test_partner_iso_filter_in_sql(self, client):
        mock = _mock_run([FLOW_ROW])
        with patch("api.main.run_query", mock):
            client.get("/v1/trade-flows?partner_iso=CHN")
        sql: str = mock.call_args[0][0]
        assert "partner_iso = 'CHN'" in sql

    def test_period_filter_in_sql(self, client):
        mock = _mock_run([FLOW_ROW])
        with patch("api.main.run_query", mock):
            client.get("/v1/trade-flows?period=2022")
        sql: str = mock.call_args[0][0]
        assert "period = '2022'" in sql

    def test_flow_code_filter_in_sql(self, client):
        mock = _mock_run([FLOW_ROW])
        with patch("api.main.run_query", mock):
            client.get("/v1/trade-flows?flow_code=M")
        sql: str = mock.call_args[0][0]
        assert "flow_code = 'M'" in sql

    def test_all_filters_combined(self, client):
        mock = _mock_run([FLOW_ROW])
        with patch("api.main.run_query", mock):
            client.get("/v1/trade-flows?reporter_iso=USA&partner_iso=CHN&period=2022&flow_code=M")
        sql: str = mock.call_args[0][0]
        assert "reporter_iso = 'USA'" in sql
        assert "partner_iso = 'CHN'" in sql
        assert "period = '2022'" in sql
        assert "flow_code = 'M'" in sql

    def test_freq_code_always_annual(self, client):
        mock = _mock_run([FLOW_ROW])
        with patch("api.main.run_query", mock):
            client.get("/v1/trade-flows")
        sql: str = mock.call_args[0][0]
        assert "freq_code = 'A'" in sql

    def test_orders_by_trade_value_desc(self, client):
        mock = _mock_run([FLOW_ROW])
        with patch("api.main.run_query", mock):
            client.get("/v1/trade-flows")
        sql: str = mock.call_args[0][0]
        assert "trade_value_usd DESC" in sql

    def test_default_limit_100_in_sql(self, client):
        mock = _mock_run([FLOW_ROW])
        with patch("api.main.run_query", mock):
            client.get("/v1/trade-flows")
        sql: str = mock.call_args[0][0]
        assert "LIMIT 100" in sql

    def test_custom_limit_in_sql(self, client):
        mock = _mock_run([FLOW_ROW])
        with patch("api.main.run_query", mock):
            client.get("/v1/trade-flows?limit=500")
        sql: str = mock.call_args[0][0]
        assert "LIMIT 500" in sql

    def test_empty_result_returns_empty_list(self, client):
        with patch("api.main.run_query", return_value=[]):
            r = client.get("/v1/trade-flows")
        assert r.status_code == 200
        assert r.json()["data"] == []

    def test_invalid_reporter_iso_returns_422(self, client):
        r = client.get("/v1/trade-flows?reporter_iso=US12")
        assert r.status_code == 422

    def test_invalid_period_returns_422(self, client):
        r = client.get("/v1/trade-flows?period=22")
        assert r.status_code == 422

    def test_limit_above_max_returns_422(self, client):
        r = client.get("/v1/trade-flows?limit=9999")
        assert r.status_code == 422

    def test_athena_error_returns_500(self, client):
        with patch("api.main.run_query", side_effect=AthenaQueryError("fail")):
            r = client.get("/v1/trade-flows")
        assert r.status_code == 500


# ── api/athena.py unit tests ──────────────────────────────────────────────────


class TestRunQuery:
    """Tests for api.athena.run_query — mocks boto3 entirely."""

    def _make_athena_client(
        self,
        execution_id: str = "exec-1",
        final_state: str = "SUCCEEDED",
        rows: list[dict] | None = None,
    ) -> MagicMock:
        """Build a mock boto3 Athena client."""
        client = MagicMock()
        client.start_query_execution.return_value = {"QueryExecutionId": execution_id}
        client.get_query_execution.return_value = {
            "QueryExecution": {
                "Status": {
                    "State": final_state,
                    "StateChangeReason": "some reason",
                }
            }
        }

        # Paginator
        result_rows = rows or [
            {"Data": [{"VarCharValue": "col_a"}, {"VarCharValue": "col_b"}]},
            {"Data": [{"VarCharValue": "val1"}, {"VarCharValue": "val2"}]},
        ]
        page = {"ResultSet": {"Rows": result_rows}}
        paginator = MagicMock()
        paginator.paginate.return_value = [page]
        client.get_paginator.return_value = paginator
        return client

    def test_returns_list_of_dicts(self):
        boto_client = self._make_athena_client()
        with (
            patch("api.athena.boto3.client", return_value=boto_client),
            patch("api.athena.time.sleep"),
        ):
            result = run_query("SELECT 1", "wg", "s3://bucket/", "us-east-1")
        assert result == [{"col_a": "val1", "col_b": "val2"}]

    def test_header_row_stripped(self):
        boto_client = self._make_athena_client()
        with (
            patch("api.athena.boto3.client", return_value=boto_client),
            patch("api.athena.time.sleep"),
        ):
            result = run_query("SELECT 1", "wg", "s3://bucket/", "us-east-1")
        # Header row ("col_a", "col_b") must not appear as a data row.
        assert not any(
            v in ("col_a", "col_b") for row in result for v in row.values()
        )

    def test_failed_query_raises_error(self):
        boto_client = self._make_athena_client(final_state="FAILED")
        with (
            patch("api.athena.boto3.client", return_value=boto_client),
            patch("api.athena.time.sleep"),
            pytest.raises(AthenaQueryError, match="FAILED"),
        ):
            run_query("SELECT 1", "wg", "s3://bucket/", "us-east-1")

    def test_cancelled_query_raises_error(self):
        boto_client = self._make_athena_client(final_state="CANCELLED")
        with (
            patch("api.athena.boto3.client", return_value=boto_client),
            patch("api.athena.time.sleep"),
            pytest.raises(AthenaQueryError, match="CANCELLED"),
        ):
            run_query("SELECT 1", "wg", "s3://bucket/", "us-east-1")

    def test_timeout_raises_error(self):
        client_mock = MagicMock()
        client_mock.start_query_execution.return_value = {"QueryExecutionId": "x"}
        client_mock.get_query_execution.return_value = {
            "QueryExecution": {"Status": {"State": "RUNNING"}}
        }
        with (
            patch("api.athena.boto3.client", return_value=client_mock),
            patch("api.athena.time.sleep"),
            pytest.raises(AthenaQueryError, match="timed out"),
        ):
            run_query("SELECT 1", "wg", "s3://bucket/", "us-east-1", max_polls=2)

    def test_passes_workgroup_to_start_execution(self):
        boto_client = self._make_athena_client()
        with (
            patch("api.athena.boto3.client", return_value=boto_client),
            patch("api.athena.time.sleep"),
        ):
            run_query("SELECT 1", "my-wg", "s3://bucket/", "us-east-1")
        call_kwargs = boto_client.start_query_execution.call_args[1]
        assert call_kwargs["WorkGroup"] == "my-wg"

    def test_passes_output_location_to_start_execution(self):
        boto_client = self._make_athena_client()
        with (
            patch("api.athena.boto3.client", return_value=boto_client),
            patch("api.athena.time.sleep"),
        ):
            run_query("SELECT 1", "wg", "s3://my-bucket/results/", "us-east-1")
        call_kwargs = boto_client.start_query_execution.call_args[1]
        assert call_kwargs["ResultConfiguration"]["OutputLocation"] == "s3://my-bucket/results/"

    def test_region_passed_to_boto_client(self):
        boto_client = self._make_athena_client()
        with (
            patch("api.athena.boto3.client", return_value=boto_client) as boto_mock,
            patch("api.athena.time.sleep"),
        ):
            run_query("SELECT 1", "wg", "s3://bucket/", "eu-west-1")
        boto_mock.assert_called_once_with("athena", region_name="eu-west-1")

    def test_empty_result_returns_empty_list(self):
        boto_client = self._make_athena_client(
            rows=[
                {"Data": [{"VarCharValue": "col_a"}]},  # header only
            ]
        )
        with (
            patch("api.athena.boto3.client", return_value=boto_client),
            patch("api.athena.time.sleep"),
        ):
            result = run_query("SELECT 1", "wg", "s3://bucket/", "us-east-1")
        assert result == []

    def test_null_cell_values_become_none(self):
        boto_client = self._make_athena_client(
            rows=[
                {"Data": [{"VarCharValue": "col_a"}]},
                {"Data": [{}]},  # cell with no VarCharValue
            ]
        )
        with (
            patch("api.athena.boto3.client", return_value=boto_client),
            patch("api.athena.time.sleep"),
        ):
            result = run_query("SELECT 1", "wg", "s3://bucket/", "us-east-1")
        assert result == [{"col_a": None}]
