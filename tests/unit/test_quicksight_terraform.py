"""
Unit tests for terraform/quicksight.tf

Validates the QuickSight Terraform configuration by parsing the HCL file as
text.  No Terraform or AWS credentials required — these are structural checks
that catch typos, missing resources, and malformed column definitions before CI
runs ``terraform validate``.
"""
from __future__ import annotations

from pathlib import Path

import pytest

QS_TF = Path(__file__).parents[2] / "terraform" / "quicksight.tf"
VARS_TF = Path(__file__).parents[2] / "terraform" / "variables.tf"
OUTPUTS_TF = Path(__file__).parents[2] / "terraform" / "outputs.tf"

_QS = QS_TF.read_text()
_VARS = VARS_TF.read_text()
_OUTPUTS = OUTPUTS_TF.read_text()

DATASETS = ["reporter_summary", "trade_flows"]

REPORTER_SUMMARY_COLUMNS = [
    "period",
    "freq_code",
    "reporter_code",
    "reporter_iso",
    "reporter_name",
    "export_value_usd",
    "import_value_usd",
    "total_trade_value_usd",
    "trade_balance_usd",
    "commodity_count",
    "partner_count",
]

TRADE_FLOWS_COLUMNS = [
    "period",
    "freq_code",
    "type_code",
    "reporter_code",
    "reporter_iso",
    "reporter_name",
    "partner_code",
    "partner_iso",
    "partner_name",
    "commodity_code",
    "commodity_name",
    "flow_code",
    "flow_name",
    "trade_value_usd",
    "source_record_count",
]


# ── Feature flag ──────────────────────────────────────────────────────────────


class TestFeatureFlag:
    def test_enable_quicksight_variable_defined(self):
        assert 'variable "enable_quicksight"' in _VARS

    def test_enable_quicksight_defaults_to_false(self):
        # Locate the variable block and check default value
        start = _VARS.index('variable "enable_quicksight"')
        end = _VARS.index("\n}", start)
        block = _VARS[start:end]
        assert "default     = false" in block

    def test_quicksight_username_variable_defined(self):
        assert 'variable "quicksight_username"' in _VARS

    def test_quicksight_username_defaults_to_empty_string(self):
        start = _VARS.index('variable "quicksight_username"')
        end = _VARS.index("\n}", start)
        block = _VARS[start:end]
        assert 'default     = ""' in block

    def test_all_qs_resources_gated(self):
        # Every resource block must include the feature-flag count expression
        resource_blocks = [
            'resource "aws_iam_role" "quicksight"',
            'resource "aws_iam_role_policy" "quicksight_data_access"',
            'resource "aws_quicksight_data_source" "athena"',
            'resource "aws_quicksight_data_set" "reporter_summary"',
            'resource "aws_quicksight_data_set" "trade_flows"',
        ]
        for block in resource_blocks:
            start = _QS.index(block)
            # Check within the first ~200 chars of each resource block
            snippet = _QS[start : start + 200]
            assert "var.enable_quicksight ? 1 : 0" in snippet, (
                f"Resource {block!r} is not gated on var.enable_quicksight"
            )


# ── IAM role ──────────────────────────────────────────────────────────────────


class TestQuickSightIamRole:
    def test_iam_role_resource_defined(self):
        assert 'resource "aws_iam_role" "quicksight"' in _QS

    def test_iam_role_uses_name_prefix(self):
        assert "local.name_prefix" in _QS

    def test_iam_role_trust_policy_uses_quicksight_principal(self):
        assert '"quicksight.amazonaws.com"' in _QS

    def test_iam_role_allows_sts_assume_role(self):
        assert '"sts:AssumeRole"' in _QS

    def test_iam_role_policy_resource_defined(self):
        assert 'resource "aws_iam_role_policy" "quicksight_data_access"' in _QS

    def test_iam_policy_has_athena_access_sid(self):
        assert '"AthenaAccess"' in _QS

    def test_iam_policy_has_s3_read_sid(self):
        assert '"S3DataLakeRead"' in _QS

    def test_iam_policy_has_s3_write_sid(self):
        assert '"S3AthenaResultsWrite"' in _QS

    def test_iam_policy_has_glue_read_sid(self):
        assert '"GlueCatalogRead"' in _QS

    def test_iam_policy_scopes_athena_to_workgroup(self):
        assert "aws_athena_workgroup.comtrade.arn" in _QS

    def test_iam_policy_scopes_s3_to_data_lake(self):
        assert "aws_s3_bucket.data_lake.arn" in _QS

    def test_iam_policy_scopes_glue_to_comtrade_database(self):
        assert '"arn:aws:glue:*:*:database/comtrade*"' in _QS


# ── Athena data source ────────────────────────────────────────────────────────


class TestQuickSightDataSource:
    def test_data_source_resource_defined(self):
        assert 'resource "aws_quicksight_data_source" "athena"' in _QS

    def test_data_source_type_is_athena(self):
        assert 'type           = "ATHENA"' in _QS

    def test_data_source_uses_project_workgroup(self):
        assert "aws_athena_workgroup.comtrade.name" in _QS

    def test_data_source_ssl_enabled(self):
        assert "disable_ssl = false" in _QS

    def test_data_source_has_permission_block(self):
        start = _QS.index('resource "aws_quicksight_data_source" "athena"')
        end = _QS.index("\n}\n", start)
        block = _QS[start:end]
        assert "permission" in block

    def test_data_source_principal_uses_qs_local(self):
        assert "local.qs_principal" in _QS

    def test_data_source_uses_caller_identity(self):
        assert "data.aws_caller_identity.current.account_id" in _QS

    def test_data_source_name_includes_environment(self):
        assert "var.environment" in _QS


# ── Dataset: reporter_summary ─────────────────────────────────────────────────


class TestReporterSummaryDataset:
    def _block(self) -> str:
        start = _QS.index('resource "aws_quicksight_data_set" "reporter_summary"')
        end = _QS.index("\n}\n", start)
        return _QS[start:end]

    def test_dataset_resource_defined(self):
        assert 'resource "aws_quicksight_data_set" "reporter_summary"' in _QS

    def test_import_mode_is_spice(self):
        assert 'import_mode    = "SPICE"' in self._block()

    def test_queries_silver_reporter_summary(self):
        assert "comtrade_silver.reporter_summary" in self._block()

    def test_filters_annual_frequency(self):
        assert "freq_code = 'A'" in self._block()

    def test_has_permission_block(self):
        assert "permission" in self._block()

    @pytest.mark.parametrize("col", REPORTER_SUMMARY_COLUMNS)
    def test_column_defined(self, col):
        assert f'name = "{col}"' in self._block()

    def test_numeric_columns_typed_correctly(self):
        block = self._block()
        # Monetary columns must be DECIMAL
        for col in ["export_value_usd", "import_value_usd", "total_trade_value_usd", "trade_balance_usd"]:
            # Find each column block and verify the type
            col_start = block.index(f'name = "{col}"')
            col_snippet = block[col_start : col_start + 60]
            assert "DECIMAL" in col_snippet

    def test_count_columns_typed_as_integer(self):
        block = self._block()
        for col in ["reporter_code", "commodity_count", "partner_count"]:
            col_start = block.index(f'name = "{col}"')
            col_snippet = block[col_start : col_start + 60]
            assert "INTEGER" in col_snippet


# ── Dataset: trade_flows ──────────────────────────────────────────────────────


class TestTradeFlowsDataset:
    def _block(self) -> str:
        start = _QS.index('resource "aws_quicksight_data_set" "trade_flows"')
        end = _QS.index("\n}\n", start)
        return _QS[start:end]

    def test_dataset_resource_defined(self):
        assert 'resource "aws_quicksight_data_set" "trade_flows"' in _QS

    def test_import_mode_is_spice(self):
        assert 'import_mode    = "SPICE"' in self._block()

    def test_queries_silver_trade_flows(self):
        assert "comtrade_silver.trade_flows" in self._block()

    def test_filters_annual_frequency(self):
        assert "freq_code = 'A'" in self._block()

    def test_has_permission_block(self):
        assert "permission" in self._block()

    @pytest.mark.parametrize("col", TRADE_FLOWS_COLUMNS)
    def test_column_defined(self, col):
        assert f'name = "{col}"' in self._block()

    def test_trade_value_typed_as_decimal(self):
        block = self._block()
        col_start = block.index('name = "trade_value_usd"')
        col_snippet = block[col_start : col_start + 60]
        assert "DECIMAL" in col_snippet

    def test_source_record_count_typed_as_integer(self):
        block = self._block()
        col_start = block.index('name = "source_record_count"')
        col_snippet = block[col_start : col_start + 60]
        assert "INTEGER" in col_snippet

    def test_both_reporter_and_partner_columns_present(self):
        block = self._block()
        for col in ["reporter_iso", "reporter_name", "partner_iso", "partner_name"]:
            assert f'name = "{col}"' in block


# ── Principal ARN construction ────────────────────────────────────────────────


class TestQsPrincipal:
    def test_qs_principal_local_defined(self):
        assert "qs_principal" in _QS

    def test_qs_principal_references_region(self):
        start = _QS.index("qs_principal")
        snippet = _QS[start : start + 150]
        assert "var.aws_region" in snippet

    def test_qs_principal_references_account_id(self):
        start = _QS.index("qs_principal")
        snippet = _QS[start : start + 150]
        assert "data.aws_caller_identity.current.account_id" in snippet

    def test_qs_principal_references_quicksight_username(self):
        start = _QS.index("qs_principal")
        snippet = _QS[start : start + 150]
        assert "var.quicksight_username" in snippet

    def test_qs_principal_arn_format(self):
        start = _QS.index("qs_principal")
        snippet = _QS[start : start + 150]
        assert "arn:aws:quicksight:" in snippet
        assert "user/default/" in snippet


# ── Outputs ───────────────────────────────────────────────────────────────────


class TestQuickSightOutputs:
    def test_data_source_arn_output_defined(self):
        assert 'output "quicksight_data_source_arn"' in _OUTPUTS

    def test_console_url_output_defined(self):
        assert 'output "quicksight_console_url"' in _OUTPUTS

    def test_data_source_arn_gated_on_flag(self):
        start = _OUTPUTS.index('output "quicksight_data_source_arn"')
        end = _OUTPUTS.index("\n}", start)
        block = _OUTPUTS[start:end]
        assert "var.enable_quicksight" in block

    def test_console_url_gated_on_flag(self):
        start = _OUTPUTS.index('output "quicksight_console_url"')
        end = _OUTPUTS.index("\n}", start)
        block = _OUTPUTS[start:end]
        assert "var.enable_quicksight" in block

    def test_console_url_references_region(self):
        start = _OUTPUTS.index('output "quicksight_console_url"')
        end = _OUTPUTS.index("\n}", start)
        block = _OUTPUTS[start:end]
        assert "var.aws_region" in block
