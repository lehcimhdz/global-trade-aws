"""
Unit tests for terraform/athena.tf

Validates the Athena Terraform configuration by parsing the HCL file as text.
No Terraform or AWS credentials required — these are structural checks that
catch typos, missing resources, and malformed SQL before CI runs `terraform validate`.
"""
from __future__ import annotations

from pathlib import Path

import pytest

ATHENA_TF = Path(__file__).parents[2] / "terraform" / "athena.tf"
IAM_TF = Path(__file__).parents[2] / "terraform" / "iam.tf"
OUTPUTS_TF = Path(__file__).parents[2] / "terraform" / "outputs.tf"

_ATHENA = ATHENA_TF.read_text()
_IAM = IAM_TF.read_text()
_OUTPUTS = OUTPUTS_TF.read_text()

# Expected named query resource names
NAMED_QUERIES = [
    "top_exporters_by_period",
    "top_commodities_by_reporter",
    "bilateral_trade_balance",
    "yoy_growth_by_reporter",
    "data_freshness_check",
]


# ── Workgroup ─────────────────────────────────────────────────────────────────


class TestAthenaWorkgroup:
    def test_workgroup_resource_defined(self):
        assert 'resource "aws_athena_workgroup" "comtrade"' in _ATHENA

    def test_workgroup_enforce_configuration_enabled(self):
        assert "enforce_workgroup_configuration    = true" in _ATHENA

    def test_workgroup_publishes_cloudwatch_metrics(self):
        assert "publish_cloudwatch_metrics_enabled = true" in _ATHENA

    def test_workgroup_has_bytes_scanned_cutoff(self):
        assert "bytes_scanned_cutoff_per_query" in _ATHENA

    def test_workgroup_results_go_to_data_lake(self):
        assert "athena-results/" in _ATHENA

    def test_workgroup_uses_sse_s3_encryption(self):
        assert "SSE_S3" in _ATHENA

    def test_workgroup_uses_name_prefix(self):
        assert "local.name_prefix" in _ATHENA

    def test_workgroup_tagged(self):
        assert "local.tags" in _ATHENA


# ── Named queries: presence ───────────────────────────────────────────────────


class TestNamedQueryPresence:
    @pytest.mark.parametrize("query_name", NAMED_QUERIES)
    def test_resource_defined(self, query_name):
        assert f'resource "aws_athena_named_query" "{query_name}"' in _ATHENA

    @pytest.mark.parametrize("query_name", NAMED_QUERIES)
    def test_references_workgroup(self, query_name):
        # Each named query block must reference the workgroup
        # (verified by checking the pattern appears the right number of times)
        assert _ATHENA.count("aws_athena_workgroup.comtrade.id") == len(NAMED_QUERIES)

    @pytest.mark.parametrize("query_name", NAMED_QUERIES)
    def test_has_description(self, query_name):
        assert "description" in _ATHENA

    @pytest.mark.parametrize("query_name", NAMED_QUERIES)
    def test_database_points_to_silver(self, query_name):
        assert "comtrade_silver" in _ATHENA


# ── Named queries: SQL correctness ───────────────────────────────────────────


class TestTopExportersQuery:
    def _sql(self):
        start = _ATHENA.index('resource "aws_athena_named_query" "top_exporters_by_period"')
        end = _ATHENA.index("\n}\n", start)
        return _ATHENA[start:end]

    def test_selects_from_reporter_summary(self):
        assert "reporter_summary" in self._sql()

    def test_orders_by_export_value(self):
        assert "export_value_usd DESC" in self._sql()

    def test_has_limit(self):
        assert "LIMIT" in self._sql()

    def test_filters_annual_freq(self):
        assert "freq_code" in self._sql()
        assert "'A'" in self._sql()

    def test_returns_balance_column(self):
        assert "trade_balance_usd" in self._sql()


class TestTopCommoditiesQuery:
    def _sql(self):
        start = _ATHENA.index('resource "aws_athena_named_query" "top_commodities_by_reporter"')
        end = _ATHENA.index("\n}\n", start)
        return _ATHENA[start:end]

    def test_selects_from_trade_flows(self):
        assert "trade_flows" in self._sql()

    def test_includes_commodity_columns(self):
        assert "commodity_code" in self._sql()
        assert "commodity_name" in self._sql()

    def test_filters_by_reporter_iso(self):
        assert "reporter_iso" in self._sql()

    def test_orders_by_trade_value(self):
        assert "trade_value_usd DESC" in self._sql()

    def test_has_limit(self):
        assert "LIMIT" in self._sql()


class TestBilateralTradeBalanceQuery:
    def _sql(self):
        start = _ATHENA.index('resource "aws_athena_named_query" "bilateral_trade_balance"')
        end = _ATHENA.index("\n}\n", start)
        return _ATHENA[start:end]

    def test_selects_from_trade_flows(self):
        assert "trade_flows" in self._sql()

    def test_filters_both_reporter_and_partner(self):
        assert "reporter_iso" in self._sql()
        assert "partner_iso" in self._sql()

    def test_includes_flow_code(self):
        assert "flow_code" in self._sql()

    def test_orders_by_period(self):
        assert "ORDER BY period" in self._sql()


class TestYoYGrowthQuery:
    def _sql(self):
        start = _ATHENA.index('resource "aws_athena_named_query" "yoy_growth_by_reporter"')
        end = _ATHENA.index("\n}\n", start)
        return _ATHENA[start:end]

    def test_uses_cte(self):
        assert "WITH" in self._sql()

    def test_has_base_and_current_ctes(self):
        assert "base AS" in self._sql()
        assert "current AS" in self._sql()

    def test_computes_growth_percentage(self):
        assert "growth_pct" in self._sql()

    def test_joins_ctes(self):
        assert "JOIN" in self._sql()

    def test_uses_nullif_for_division_safety(self):
        assert "NULLIF" in self._sql()


class TestDataFreshnessQuery:
    def _sql(self):
        start = _ATHENA.index('resource "aws_athena_named_query" "data_freshness_check"')
        end = _ATHENA.index("\n}\n", start)
        return _ATHENA[start:end]

    def test_selects_from_reporter_summary(self):
        assert "reporter_summary" in self._sql()

    def test_uses_max_period(self):
        assert "MAX(period)" in self._sql()

    def test_groups_by_reporter(self):
        assert "GROUP BY" in self._sql()
        assert "reporter_iso" in self._sql()


# ── IAM: Athena permissions ───────────────────────────────────────────────────


class TestAthenaIamPermissions:
    def test_athena_query_execution_sid_present(self):
        assert "AthenaQueryExecution" in _IAM

    def test_athena_start_query_permission(self):
        assert "athena:StartQueryExecution" in _IAM

    def test_athena_get_results_permission(self):
        assert "athena:GetQueryResults" in _IAM

    def test_athena_workgroup_scoped(self):
        assert "workgroup/${local.name_prefix}-comtrade" in _IAM

    def test_athena_results_bucket_sid_present(self):
        assert "AthenaResultsBucket" in _IAM

    def test_athena_results_s3_permissions(self):
        assert "athena-results/*" in _IAM


# ── Outputs ───────────────────────────────────────────────────────────────────


class TestAthenaOutputs:
    def test_workgroup_name_output_defined(self):
        assert 'output "athena_workgroup_name"' in _OUTPUTS

    def test_named_queries_output_defined(self):
        assert 'output "athena_named_queries"' in _OUTPUTS

    @pytest.mark.parametrize("query_name", NAMED_QUERIES)
    def test_each_query_in_output(self, query_name):
        assert query_name in _OUTPUTS
