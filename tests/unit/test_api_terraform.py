"""
Unit tests for terraform/api.tf

Validates the trade-API Terraform configuration by parsing HCL as text.
No Terraform or AWS credentials required.
"""
from __future__ import annotations

from pathlib import Path

import pytest

API_TF = Path(__file__).parents[2] / "terraform" / "api.tf"
VARS_TF = Path(__file__).parents[2] / "terraform" / "variables.tf"
OUTPUTS_TF = Path(__file__).parents[2] / "terraform" / "outputs.tf"

_API = API_TF.read_text()
_VARS = VARS_TF.read_text()
_OUTPUTS = OUTPUTS_TF.read_text()


# ── Feature flag ──────────────────────────────────────────────────────────────


class TestFeatureFlag:
    def test_enable_api_variable_defined(self):
        assert 'variable "enable_api"' in _VARS

    def test_enable_api_defaults_false(self):
        start = _VARS.index('variable "enable_api"')
        end = _VARS.index("\n}", start)
        block = _VARS[start:end]
        assert "default     = false" in block

    def test_api_lambda_layer_arn_variable_defined(self):
        assert 'variable "api_lambda_layer_arn"' in _VARS

    def test_api_lambda_layer_arn_defaults_empty(self):
        start = _VARS.index('variable "api_lambda_layer_arn"')
        end = _VARS.index("\n}", start)
        block = _VARS[start:end]
        assert 'default     = ""' in block

    def _gated_resources(self):
        return [
            'resource "aws_iam_role" "api_lambda"',
            'resource "aws_iam_role_policy_attachment" "api_lambda_basic"',
            'resource "aws_iam_role_policy" "api_lambda_data_access"',
            'resource "aws_lambda_function" "api"',
            'resource "aws_lambda_function_url" "api"',
        ]

    @pytest.mark.parametrize(
        "resource",
        [
            'resource "aws_iam_role" "api_lambda"',
            'resource "aws_iam_role_policy_attachment" "api_lambda_basic"',
            'resource "aws_iam_role_policy" "api_lambda_data_access"',
            'resource "aws_lambda_function" "api"',
            'resource "aws_lambda_function_url" "api"',
        ],
    )
    def test_resource_gated_on_enable_api(self, resource):
        start = _API.index(resource)
        snippet = _API[start : start + 200]
        assert "var.enable_api ? 1 : 0" in snippet


# ── IAM role ──────────────────────────────────────────────────────────────────


class TestApiIamRole:
    def test_iam_role_resource_defined(self):
        assert 'resource "aws_iam_role" "api_lambda"' in _API

    def test_iam_role_uses_name_prefix(self):
        start = _API.index('resource "aws_iam_role" "api_lambda"')
        end = _API.index("\n}\n", start)
        assert "local.name_prefix" in _API[start:end]

    def test_trust_policy_lambda_principal(self):
        assert '"lambda.amazonaws.com"' in _API

    def test_trust_policy_sts_assume_role(self):
        assert '"sts:AssumeRole"' in _API

    def test_basic_execution_policy_attached(self):
        assert "AWSLambdaBasicExecutionRole" in _API

    def test_inline_policy_has_athena_sid(self):
        assert '"AthenaQueryExecution"' in _API

    def test_inline_policy_has_s3_read_sid(self):
        assert '"S3DataLakeRead"' in _API

    def test_inline_policy_has_s3_write_sid(self):
        assert '"S3AthenaResultsWrite"' in _API

    def test_inline_policy_has_glue_sid(self):
        assert '"GlueCatalogRead"' in _API

    def test_athena_scoped_to_workgroup(self):
        assert "aws_athena_workgroup.comtrade.arn" in _API

    def test_s3_scoped_to_data_lake(self):
        assert "aws_s3_bucket.data_lake.arn" in _API

    def test_glue_scoped_to_comtrade_database(self):
        assert '"arn:aws:glue:*:*:database/comtrade*"' in _API


# ── Lambda function ───────────────────────────────────────────────────────────


class TestLambdaFunction:
    def _block(self) -> str:
        start = _API.index('resource "aws_lambda_function" "api"')
        end = _API.index("\n}\n", start)
        return _API[start:end]

    def test_resource_defined(self):
        assert 'resource "aws_lambda_function" "api"' in _API

    def test_runtime_python311(self):
        assert 'runtime          = "python3.11"' in self._block()

    def test_handler_is_main_handler(self):
        assert 'handler          = "main.handler"' in self._block()

    def test_timeout_set(self):
        assert "timeout" in self._block()

    def test_memory_size_set(self):
        assert "memory_size" in self._block()

    def test_env_var_athena_workgroup(self):
        assert "ATHENA_WORKGROUP" in self._block()

    def test_env_var_output_location(self):
        assert "ATHENA_OUTPUT_LOCATION" in self._block()

    def test_env_var_aws_region(self):
        assert "AWS_DEFAULT_REGION" in self._block()

    def test_output_location_points_to_athena_results(self):
        assert "athena-results/" in self._block()

    def test_source_code_hash_tracks_changes(self):
        assert "source_code_hash" in self._block()

    def test_uses_name_prefix(self):
        assert "local.name_prefix" in self._block()

    def test_uses_prebuilt_zip(self):
        block = self._block()
        assert "local.api_zip_path" in block
        assert "filebase64sha256(local.api_zip_path)" in block


# ── Deployment package ────────────────────────────────────────────────────────


class TestDeploymentPackage:
    def test_no_archive_file_data_source(self):
        # The archive_file data source was removed because it bundled api/*.py
        # without the FastAPI/Mangum dependencies, clobbering make api-build.
        assert 'data "archive_file" "api"' not in _API

    def test_api_zip_path_local_defined(self):
        assert "api_zip_path" in _API
        assert "../build/api.zip" in _API


# ── Function URL ──────────────────────────────────────────────────────────────


class TestFunctionUrl:
    def _block(self) -> str:
        start = _API.index('resource "aws_lambda_function_url" "api"')
        end = _API.index("\n}\n", start)
        return _API[start:end]

    def test_function_url_resource_defined(self):
        assert 'resource "aws_lambda_function_url" "api"' in _API

    def test_references_lambda_function(self):
        assert "aws_lambda_function.api[0].function_name" in self._block()

    def test_cors_block_present(self):
        assert "cors" in self._block()

    def test_cors_allows_get(self):
        assert '"GET"' in self._block()

    def test_auth_type_from_variable(self):
        assert "authorization_type = var.api_function_url_auth_type" in self._block()

    def test_cors_origins_from_variable(self):
        assert "var.api_function_url_allowed_origins" in self._block()

    def test_auth_type_variable_defined(self):
        assert 'variable "api_function_url_auth_type"' in _VARS

    def test_auth_type_defaults_to_none(self):
        start = _VARS.index('variable "api_function_url_auth_type"')
        end = _VARS.index("\n}", start)
        block = _VARS[start:end]
        assert 'default     = "NONE"' in block

    def test_auth_type_validation_restricts_to_known_values(self):
        start = _VARS.index('variable "api_function_url_auth_type"')
        end = _VARS.index("\n}", start)
        block = _VARS[start:end]
        assert '["NONE", "AWS_IAM"]' in block

    def test_allowed_origins_variable_defined(self):
        assert 'variable "api_function_url_allowed_origins"' in _VARS

    def test_allowed_origins_defaults_to_wildcard(self):
        start = _VARS.index('variable "api_function_url_allowed_origins"')
        end = _VARS.index("\n}", start)
        block = _VARS[start:end]
        assert '["*"]' in block


# ── Outputs ───────────────────────────────────────────────────────────────────


class TestApiOutputs:
    def test_api_endpoint_url_output_defined(self):
        assert 'output "api_endpoint_url"' in _OUTPUTS

    def test_api_endpoint_url_gated_on_flag(self):
        start = _OUTPUTS.index('output "api_endpoint_url"')
        end = _OUTPUTS.index("\n}", start)
        block = _OUTPUTS[start:end]
        assert "var.enable_api" in block

    def test_api_endpoint_url_references_function_url(self):
        start = _OUTPUTS.index('output "api_endpoint_url"')
        end = _OUTPUTS.index("\n}", start)
        block = _OUTPUTS[start:end]
        assert "aws_lambda_function_url.api" in block
