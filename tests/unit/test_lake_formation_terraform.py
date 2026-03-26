"""
Unit tests for terraform/lake_formation.tf

Validates the Lake Formation access control configuration by parsing the HCL
file as text.  No Terraform or AWS credentials required.
"""
from __future__ import annotations

from pathlib import Path

LF_TF = Path(__file__).parents[2] / "terraform" / "lake_formation.tf"

_LF = LF_TF.read_text()


# ── Data location ─────────────────────────────────────────────────────────────


class TestDataLocation:
    def test_resource_defined(self):
        assert 'resource "aws_lakeformation_resource" "data_lake"' in _LF

    def test_references_data_lake_bucket(self):
        assert "aws_s3_bucket.data_lake.arn" in _LF


# ── LF tags ───────────────────────────────────────────────────────────────────


class TestLfTags:
    def test_env_tag_defined(self):
        assert 'resource "aws_lakeformation_lf_tag" "env"' in _LF

    def test_classification_tag_defined(self):
        assert 'resource "aws_lakeformation_lf_tag" "classification"' in _LF

    def test_env_tag_has_three_values(self):
        assert '"dev"' in _LF
        assert '"staging"' in _LF
        assert '"prod"' in _LF

    def test_classification_tag_has_expected_values(self):
        assert '"public"' in _LF
        assert '"internal"' in _LF
        assert '"restricted"' in _LF

    def test_database_tag_association_defined(self):
        assert 'resource "aws_lakeformation_lf_tags_lf_tag_association" "database"' in _LF

    def test_database_tagged_with_env_variable(self):
        assert "var.environment" in _LF


# ── Airflow permissions ───────────────────────────────────────────────────────


class TestAirflowPermissions:
    def test_database_describe_permission_defined(self):
        assert 'resource "aws_lakeformation_permissions" "airflow_database"' in _LF

    def test_airflow_role_referenced(self):
        assert "aws_iam_role.airflow.arn" in _LF

    def test_trade_flows_permission_defined(self):
        assert 'resource "aws_lakeformation_permissions" "airflow_trade_flows"' in _LF

    def test_reporter_summary_permission_defined(self):
        assert 'resource "aws_lakeformation_permissions" "airflow_reporter_summary"' in _LF

    def test_airflow_has_select_and_alter(self):
        assert '"SELECT"' in _LF
        assert '"ALTER"' in _LF


# ── MWAA permissions ──────────────────────────────────────────────────────────


class TestMwaaPermissions:
    def test_mwaa_trade_flows_permission_defined(self):
        assert 'resource "aws_lakeformation_permissions" "mwaa_trade_flows"' in _LF

    def test_mwaa_reporter_summary_permission_defined(self):
        assert 'resource "aws_lakeformation_permissions" "mwaa_reporter_summary"' in _LF

    def test_mwaa_permission_gated_on_enable_mwaa(self):
        assert "var.enable_mwaa" in _LF

    def test_mwaa_role_referenced(self):
        assert "aws_iam_role.mwaa[0].arn" in _LF


# ── API Lambda permissions ────────────────────────────────────────────────────


class TestApiPermissions:
    def test_api_trade_flows_permission_defined(self):
        assert 'resource "aws_lakeformation_permissions" "api_trade_flows"' in _LF

    def test_api_reporter_summary_permission_defined(self):
        assert 'resource "aws_lakeformation_permissions" "api_reporter_summary"' in _LF

    def test_api_permission_gated_on_enable_api(self):
        assert "var.enable_api" in _LF

    def test_api_uses_column_level_access_on_trade_flows(self):
        assert "table_with_columns" in _LF

    def test_api_trade_flows_includes_reporter_iso(self):
        assert '"reporter_iso"' in _LF

    def test_api_trade_flows_includes_trade_value_usd(self):
        assert '"trade_value_usd"' in _LF

    def test_api_lambda_role_referenced(self):
        assert "aws_iam_role.api_lambda[0].arn" in _LF


# ── QuickSight permissions ────────────────────────────────────────────────────


class TestQuickSightPermissions:
    def test_quicksight_trade_flows_permission_defined(self):
        assert 'resource "aws_lakeformation_permissions" "quicksight_trade_flows"' in _LF

    def test_quicksight_reporter_summary_permission_defined(self):
        assert 'resource "aws_lakeformation_permissions" "quicksight_reporter_summary"' in _LF

    def test_quicksight_permission_gated_on_enable_quicksight(self):
        assert "var.enable_quicksight" in _LF

    def test_quicksight_role_referenced(self):
        assert "aws_iam_role.quicksight[0].arn" in _LF

    def test_quicksight_has_select_permission(self):
        assert '"DESCRIBE"' in _LF
