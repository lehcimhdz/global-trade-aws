# ── Trade API — Lambda + Function URL ─────────────────────────────────────────
#
# Deploys the FastAPI application (api/) as an AWS Lambda function exposed
# via a Lambda Function URL (no API Gateway required).
#
# Architecture:
#   Client → Lambda Function URL → Lambda (FastAPI + Mangum) → Athena → S3
#
# All resources are gated on var.enable_api (default: false).
# To activate:
#   1. Build the deployment package:  make api-build
#   2. Apply:  terraform apply -var="enable_api=true"
#
# The function URL is emitted as the ``api_endpoint_url`` Terraform output.

# ── Deployment package ────────────────────────────────────────────────────────

data "archive_file" "api" {
  count       = var.enable_api ? 1 : 0
  type        = "zip"
  source_dir  = "${path.module}/../api"
  output_path = "${path.module}/../build/api.zip"
  excludes    = ["__pycache__", "*.pyc", "requirements.txt"]
}

# ── IAM execution role ────────────────────────────────────────────────────────

resource "aws_iam_role" "api_lambda" {
  count       = var.enable_api ? 1 : 0
  name        = "${local.name_prefix}-trade-api"
  description = "Execution role for the trade API Lambda — Athena + S3 + Glue + CloudWatch Logs"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = local.tags
}

resource "aws_iam_role_policy_attachment" "api_lambda_basic" {
  count      = var.enable_api ? 1 : 0
  role       = aws_iam_role.api_lambda[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "api_lambda_data_access" {
  count = var.enable_api ? 1 : 0
  name  = "trade-api-data-access"
  role  = aws_iam_role.api_lambda[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AthenaQueryExecution"
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:StopQueryExecution",
          "athena:GetWorkGroup",
        ]
        Resource = [aws_athena_workgroup.comtrade.arn]
      },
      {
        Sid    = "S3DataLakeRead"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:GetBucketLocation",
        ]
        Resource = [
          aws_s3_bucket.data_lake.arn,
          "${aws_s3_bucket.data_lake.arn}/*",
        ]
      },
      {
        Sid    = "S3AthenaResultsWrite"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:AbortMultipartUpload",
        ]
        Resource = ["${aws_s3_bucket.data_lake.arn}/athena-results/*"]
      },
      {
        Sid    = "GlueCatalogRead"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions",
        ]
        Resource = [
          "arn:aws:glue:*:*:catalog",
          "arn:aws:glue:*:*:database/comtrade*",
          "arn:aws:glue:*:*:table/comtrade*/*",
        ]
      },
    ]
  })
}

# ── Lambda function ───────────────────────────────────────────────────────────

resource "aws_lambda_function" "api" {
  count = var.enable_api ? 1 : 0

  function_name    = "${local.name_prefix}-trade-api"
  description      = "Global Trade REST API — FastAPI on Lambda, queries Athena silver tables"
  filename         = data.archive_file.api[0].output_path
  source_code_hash = data.archive_file.api[0].output_base64sha256
  role             = aws_iam_role.api_lambda[0].arn
  handler          = "main.handler"
  runtime          = "python3.11"
  timeout          = 30
  memory_size      = 256

  layers = var.api_lambda_layer_arn != "" ? [var.api_lambda_layer_arn] : []

  environment {
    variables = {
      ATHENA_WORKGROUP       = aws_athena_workgroup.comtrade.name
      ATHENA_OUTPUT_LOCATION = "s3://${aws_s3_bucket.data_lake.id}/athena-results/"
      AWS_DEFAULT_REGION     = var.aws_region
    }
  }

  tags = local.tags
}

# ── Function URL (public, no auth) ────────────────────────────────────────────
# For production, change authorization_type to "AWS_IAM" and sign requests
# with SigV4, or front the function with API Gateway + Cognito.

resource "aws_lambda_function_url" "api" {
  count = var.enable_api ? 1 : 0

  function_name      = aws_lambda_function.api[0].function_name
  authorization_type = "NONE"

  cors {
    allow_credentials = false
    allow_origins     = ["*"]
    allow_methods     = ["GET"]
    allow_headers     = ["Content-Type"]
    max_age           = 300
  }
}
