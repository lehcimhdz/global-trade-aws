"""
Unit tests for terraform/cloudwatch.tf

Validates the CloudWatch dashboard and alarm configuration by parsing the HCL
file as text.  No Terraform or AWS credentials required.
"""
from __future__ import annotations

from pathlib import Path

import pytest

CW_TF = Path(__file__).parents[2] / "terraform" / "cloudwatch.tf"
VARS_TF = Path(__file__).parents[2] / "terraform" / "variables.tf"

_CW = CW_TF.read_text()
_VARS = VARS_TF.read_text()


# ── Dashboard resource ────────────────────────────────────────────────────────


class TestDashboardResource:
    def test_dashboard_resource_defined(self):
        assert 'resource "aws_cloudwatch_dashboard" "comtrade_pipeline"' in _CW

    def test_uses_name_prefix(self):
        assert "local.name_prefix" in _CW

    def test_namespace_local_defined(self):
        assert 'cw_namespace   = "Comtrade/Pipeline"' in _CW


# ── Ingestion widgets (validate_bronze metrics) ───────────────────────────────


class TestIngestionWidgets:
    def test_row_count_widget_present(self):
        assert "Row Count per DAG Run" in _CW

    def test_quality_gate_widget_present(self):
        assert "Quality Gate" in _CW

    def test_failure_rate_widget_present(self):
        assert "Check Failure Rate" in _CW

    def test_json_bytes_widget_present(self):
        assert "JSON Bytes Written to S3" in _CW

    def test_row_count_metric_searched(self):
        assert 'MetricName=\\"RowCount\\"' in _CW or "RowCount" in _CW

    def test_checks_passed_metric_searched(self):
        assert "ChecksPassed" in _CW

    def test_checks_failed_metric_searched(self):
        assert "ChecksFailed" in _CW

    def test_json_bytes_metric_searched(self):
        assert "JsonBytesWritten" in _CW


# ── dbt widgets ───────────────────────────────────────────────────────────────


class TestDbtWidgets:
    def test_dbt_duration_widget_present(self):
        assert "dbt Run Duration" in _CW

    def test_dbt_errors_widget_present(self):
        assert "dbt Errors and Test Failures" in _CW

    def test_dbt_duration_metric_searched(self):
        assert "DbtRunDuration" in _CW

    def test_dbt_models_errored_metric_searched(self):
        assert "DbtModelsErrored" in _CW

    def test_dbt_tests_failed_metric_searched(self):
        assert "DbtTestsFailed" in _CW

    def test_dbt_widgets_use_phase_dimension(self):
        assert "DagId,Phase" in _CW

    def test_dbt_duration_uses_average_statistic(self):
        assert "'Average'" in _CW


# ── Athena bytes scanned alarm ────────────────────────────────────────────────


class TestAthenaAlarm:
    def test_alarm_resource_defined(self):
        assert 'resource "aws_cloudwatch_metric_alarm" "athena_bytes_scanned"' in _CW

    def test_alarm_uses_processed_bytes_metric(self):
        assert 'metric_name         = "ProcessedBytes"' in _CW

    def test_alarm_uses_athena_namespace(self):
        assert 'namespace           = "AWS/Athena"' in _CW

    def test_alarm_scoped_to_comtrade_workgroup(self):
        assert "aws_athena_workgroup.comtrade.id" in _CW

    def test_alarm_uses_sum_statistic(self):
        assert 'statistic           = "Sum"' in _CW

    def test_alarm_uses_five_minute_period(self):
        assert "period              = 300" in _CW

    def test_alarm_threshold_from_variable(self):
        assert "var.athena_bytes_scanned_alarm_gb" in _CW

    def test_alarm_treat_missing_data_not_breaching(self):
        assert 'treat_missing_data = "notBreaching"' in _CW

    def test_alarm_greater_than_threshold_operator(self):
        assert 'comparison_operator = "GreaterThanThreshold"' in _CW

    def test_alarm_tagged(self):
        assert "local.tags" in _CW


# ── Variables ─────────────────────────────────────────────────────────────────


class TestCloudWatchVariables:
    def test_athena_bytes_scanned_alarm_gb_variable_defined(self):
        assert 'variable "athena_bytes_scanned_alarm_gb"' in _VARS

    def test_athena_alarm_default_is_five(self):
        assert "default     = 5" in _VARS
