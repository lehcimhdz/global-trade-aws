"""
Unit tests for terraform/macie.tf

Validates the Amazon Macie PII scanning configuration by parsing the HCL file
as text.  No Terraform or AWS credentials required.
"""
from __future__ import annotations

from pathlib import Path

MACIE_TF = Path(__file__).parents[2] / "terraform" / "macie.tf"

_MACIE = MACIE_TF.read_text()


# ── Macie session ─────────────────────────────────────────────────────────────


class TestMacieSession:
    def test_account_resource_defined(self):
        assert 'resource "aws_macie2_account" "main"' in _MACIE

    def test_account_enabled(self):
        assert 'status                       = "ENABLED"' in _MACIE

    def test_finding_publishing_frequency_set(self):
        assert "finding_publishing_frequency" in _MACIE


# ── KMS key ───────────────────────────────────────────────────────────────────


class TestKmsKey:
    def test_kms_key_resource_defined(self):
        assert 'resource "aws_kms_key" "macie_findings"' in _MACIE

    def test_kms_key_rotation_enabled(self):
        assert "enable_key_rotation     = true" in _MACIE

    def test_kms_alias_defined(self):
        assert 'resource "aws_kms_alias" "macie_findings"' in _MACIE


# ── Findings bucket ───────────────────────────────────────────────────────────


class TestFindingsBucket:
    def test_findings_bucket_defined(self):
        assert 'resource "aws_s3_bucket" "macie_findings"' in _MACIE

    def test_findings_bucket_encrypted(self):
        assert 'resource "aws_s3_bucket_server_side_encryption_configuration" "macie_findings"' in _MACIE

    def test_findings_bucket_public_access_blocked(self):
        assert 'resource "aws_s3_bucket_public_access_block" "macie_findings"' in _MACIE

    def test_findings_bucket_has_lifecycle(self):
        assert 'resource "aws_s3_bucket_lifecycle_configuration" "macie_findings"' in _MACIE

    def test_findings_expire_after_one_year(self):
        assert "days = 365" in _MACIE

    def test_bucket_policy_allows_macie_service(self):
        assert '"macie.amazonaws.com"' in _MACIE

    def test_bucket_policy_scoped_to_account(self):
        assert "aws:SourceAccount" in _MACIE

    def test_bucket_policy_uses_caller_identity(self):
        assert "data.aws_caller_identity.current.account_id" in _MACIE


# ── Classification job ────────────────────────────────────────────────────────


class TestClassificationJob:
    def test_job_resource_defined(self):
        assert 'resource "aws_macie2_classification_job" "data_lake"' in _MACIE

    def test_job_type_is_scheduled(self):
        assert 'job_type   = "SCHEDULED"' in _MACIE

    def test_job_is_running(self):
        assert 'job_status = "RUNNING"' in _MACIE

    def test_job_runs_monthly(self):
        assert "monthly_schedule" in _MACIE

    def test_job_runs_on_first_of_month(self):
        assert "day_of_month = 1" in _MACIE

    def test_job_targets_data_lake_bucket(self):
        assert "aws_s3_bucket.data_lake.id" in _MACIE

    def test_job_scoped_to_comtrade_prefix(self):
        assert '"comtrade/"' in _MACIE

    def test_job_uses_object_key_scope(self):
        assert "OBJECT_KEY" in _MACIE

    def test_job_depends_on_macie_account(self):
        assert "aws_macie2_account.main" in _MACIE


# ── Findings export ───────────────────────────────────────────────────────────


class TestFindingsExport:
    def test_export_configuration_defined(self):
        assert 'resource "aws_macie2_classification_export_configuration" "findings"' in _MACIE

    def test_export_goes_to_findings_bucket(self):
        assert "aws_s3_bucket.macie_findings.bucket" in _MACIE

    def test_export_uses_kms_key(self):
        assert "aws_kms_key.macie_findings.arn" in _MACIE

    def test_export_has_key_prefix(self):
        assert '"findings/"' in _MACIE


# ── Findings filter ───────────────────────────────────────────────────────────


class TestFindingsFilter:
    def test_filter_resource_defined(self):
        assert 'resource "aws_macie2_findings_filter"' in _MACIE

    def test_filter_uses_noop_action(self):
        assert '"NOOP"' in _MACIE

    def test_filter_targets_severity(self):
        assert "severity.description" in _MACIE
