# ── Least-privilege S3 policy ─────────────────────────────────────────────────

resource "aws_iam_policy" "airflow_data_lake" {
  name        = "${local.name_prefix}-airflow-data-lake"
  description = "Allows Airflow workers to read/write Comtrade data in S3"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ListDataLakeBucket"
        Effect = "Allow"
        Action = ["s3:ListBucket"]
        Resource = [aws_s3_bucket.data_lake.arn]
        Condition = {
          StringLike = {
            "s3:prefix" = ["comtrade/*"]
          }
        }
      },
      {
        Sid    = "ReadWriteComtradeObjects"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:DeleteObject",
        ]
        Resource = ["${aws_s3_bucket.data_lake.arn}/comtrade/*"]
      },
      {
        Sid    = "ReadSecretsManagerVariables"
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue",
          "secretsmanager:DescribeSecret",
        ]
        Resource = [
          for s in aws_secretsmanager_secret.airflow_variable : s.arn
        ]
      },
      {
        # cloudwatch:PutMetricData does not support resource-level restrictions.
        Sid      = "EmitCloudWatchMetrics"
        Effect   = "Allow"
        Action   = ["cloudwatch:PutMetricData"]
        Resource = ["*"]
        Condition = {
          StringEquals = {
            "cloudwatch:namespace" = "Comtrade/Pipeline"
          }
        }
      },
    ]
  })
}

# ── IAM role for Airflow on EC2 / ECS ────────────────────────────────────────

resource "aws_iam_role" "airflow" {
  name        = "${local.name_prefix}-airflow"
  description = "Role assumed by Airflow workers running on EC2 or ECS"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "ec2.amazonaws.com" }
        Action    = "sts:AssumeRole"
      },
      # Also allow ECS tasks to assume this role
      {
        Effect    = "Allow"
        Principal = { Service = "ecs-tasks.amazonaws.com" }
        Action    = "sts:AssumeRole"
      },
    ]
  })
}

resource "aws_iam_role_policy_attachment" "airflow_data_lake" {
  role       = aws_iam_role.airflow.name
  policy_arn = aws_iam_policy.airflow_data_lake.arn
}

resource "aws_iam_instance_profile" "airflow" {
  name = "${local.name_prefix}-airflow"
  role = aws_iam_role.airflow.name
}

# ── IAM user for local / Docker Compose development ──────────────────────────
# Only created in the dev environment — prod uses the IAM role above.

resource "aws_iam_user" "airflow_dev" {
  count = var.environment == "dev" ? 1 : 0
  name  = "${local.name_prefix}-airflow-dev"

  tags = {
    Purpose = "Local and Docker Compose development credentials"
  }
}

resource "aws_iam_user_policy_attachment" "airflow_dev" {
  count      = var.environment == "dev" ? 1 : 0
  user       = aws_iam_user.airflow_dev[0].name
  policy_arn = aws_iam_policy.airflow_data_lake.arn
}

# NOTE: Do not create aws_iam_access_key here — access keys end up in
# Terraform state. Create them manually in the AWS Console and store the
# values via `make bootstrap-secrets`.
