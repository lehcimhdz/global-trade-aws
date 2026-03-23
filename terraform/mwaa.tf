# ── AWS MWAA — Managed Airflow ────────────────────────────────────────────────
#
# All resources gated on var.enable_mwaa.
# In dev, Docker Compose is used; in staging/prod, MWAA replaces it.
#
# Deployment sequence (handled by CI deploy workflow):
#   1. zip plugins/ → s3://mwaa-artifacts/plugins.zip
#   2. upload requirements.txt → s3://mwaa-artifacts/requirements.txt
#   3. sync dags/ → s3://mwaa-artifacts/dags/
#   4. terraform apply (updates environment if config changed)
# ─────────────────────────────────────────────────────────────────────────────

# ── S3 bucket for MWAA artifacts (DAGs, plugins, requirements) ───────────────

resource "aws_s3_bucket" "mwaa_artifacts" {
  count = var.enable_mwaa ? 1 : 0

  bucket = "${local.name_prefix}-mwaa-artifacts"

  tags = {
    Name = "${local.name_prefix}-mwaa-artifacts"
    Role = "mwaa-artifacts"
  }
}

resource "aws_s3_bucket_versioning" "mwaa_artifacts" {
  count = var.enable_mwaa ? 1 : 0

  bucket = aws_s3_bucket.mwaa_artifacts[0].id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "mwaa_artifacts" {
  count = var.enable_mwaa ? 1 : 0

  bucket = aws_s3_bucket.mwaa_artifacts[0].id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "mwaa_artifacts" {
  count = var.enable_mwaa ? 1 : 0

  bucket                  = aws_s3_bucket.mwaa_artifacts[0].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── IAM execution role for MWAA ───────────────────────────────────────────────

resource "aws_iam_role" "mwaa" {
  count = var.enable_mwaa ? 1 : 0

  name        = "${local.name_prefix}-mwaa-execution"
  description = "Execution role assumed by MWAA workers"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "airflow.amazonaws.com" }
        Action    = "sts:AssumeRole"
      },
      {
        Effect    = "Allow"
        Principal = { Service = "airflow-env.amazonaws.com" }
        Action    = "sts:AssumeRole"
      },
    ]
  })
}

resource "aws_iam_role_policy" "mwaa" {
  count = var.enable_mwaa ? 1 : 0

  name = "${local.name_prefix}-mwaa-execution-policy"
  role = aws_iam_role.mwaa[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # Required: MWAA metrics endpoint
      {
        Sid      = "MWAAPublishMetrics"
        Effect   = "Allow"
        Action   = ["airflow:PublishMetrics"]
        Resource = "arn:aws:airflow:${var.aws_region}:*:environment/${local.name_prefix}-airflow"
      },
      # DAGs, plugins, requirements — read-only
      {
        Sid    = "MWAAArtifactsRead"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:GetBucketVersioning",
          "s3:ListBucket",
        ]
        Resource = [
          aws_s3_bucket.mwaa_artifacts[0].arn,
          "${aws_s3_bucket.mwaa_artifacts[0].arn}/*",
        ]
      },
      # MWAA writes task logs to S3
      {
        Sid    = "MWAALogsWrite"
        Effect = "Allow"
        Action = ["s3:PutObject"]
        Resource = "${aws_s3_bucket.mwaa_artifacts[0].arn}/logs/*"
      },
      # CloudWatch Logs (task logs are also forwarded there)
      {
        Sid    = "MWAACloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:GetLogEvents",
          "logs:GetLogRecord",
          "logs:GetLogDelivery",
          "logs:ListLogDeliveries",
          "logs:DescribeLogGroups",
        ]
        Resource = "arn:aws:logs:${var.aws_region}:*:log-group:airflow-*"
      },
      # Secrets Manager — Airflow Variables and Connections
      {
        Sid    = "MWAASecretsManager"
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue",
          "secretsmanager:DescribeSecret",
        ]
        Resource = [
          for s in aws_secretsmanager_secret.airflow_variable : s.arn
        ]
      },
    ]
  })
}

# Attach the existing data-lake policy so MWAA workers can read/write Comtrade data.
resource "aws_iam_role_policy_attachment" "mwaa_data_lake" {
  count = var.enable_mwaa ? 1 : 0

  role       = aws_iam_role.mwaa[0].name
  policy_arn = aws_iam_policy.airflow_data_lake.arn
}

# ── MWAA environment ──────────────────────────────────────────────────────────

resource "aws_mwaa_environment" "main" {
  count = var.enable_mwaa ? 1 : 0

  name              = "${local.name_prefix}-airflow"
  airflow_version   = "2.9.3"
  environment_class = var.mwaa_environment_class
  min_workers       = var.mwaa_min_workers
  max_workers       = var.mwaa_max_workers

  # Artifact locations in S3
  source_bucket_arn    = aws_s3_bucket.mwaa_artifacts[0].arn
  dag_s3_path          = "dags/"
  plugins_s3_path      = "plugins.zip"
  requirements_s3_path = "requirements.txt"

  execution_role_arn = aws_iam_role.mwaa[0].arn

  network_configuration {
    security_group_ids = [aws_security_group.mwaa[0].id]
    subnet_ids         = aws_subnet.private[*].id
  }

  logging_configuration {
    dag_processing_logs {
      enabled   = true
      log_level = "WARNING"
    }
    scheduler_logs {
      enabled   = true
      log_level = "WARNING"
    }
    task_logs {
      enabled   = true
      log_level = "INFO"
    }
    webserver_logs {
      enabled   = true
      log_level = "WARNING"
    }
    worker_logs {
      enabled   = true
      log_level = "WARNING"
    }
  }

  airflow_configuration_options = {
    # Route Airflow Variables and Connections through Secrets Manager.
    "secrets.backend" = "airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend"
    "secrets.backend_kwargs" = jsonencode({
      connections_prefix = "airflow/connections"
      variables_prefix   = "airflow/variables"
    })

    # Disable example DAGs — keeps the UI clean.
    "core.load_examples" = "false"

    # Webserver authentication is handled by MWAA (IAM + Cognito).
    "webserver.expose_config" = "false"
  }

  depends_on = [
    aws_s3_bucket_versioning.mwaa_artifacts,
    aws_s3_bucket_public_access_block.mwaa_artifacts,
  ]
}
