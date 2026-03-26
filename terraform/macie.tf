# ── Amazon Macie — automated PII scanning ────────────────────────────────────
#
# Verifies that the Comtrade data lake contains no Personally Identifiable
# Information (PII).  Trade statistics are country-level aggregates, so Macie
# findings here indicate either a data ingestion anomaly or a misconfigured
# data source.
#
# Resources
# ---------
# * aws_macie2_account          — enables the Macie service for the account
# * aws_macie2_classification_job — monthly scan of the data lake bucket
# * aws_s3_bucket / policy      — findings export destination (separate bucket,
#                                  not the data lake itself)
#
# Design notes
# ------------
# * The classification job runs on the 1st of every month, aligned with the
#   Comtrade ingestion schedule (@monthly DAGs).
# * Only the "comtrade/" prefix is scanned — MWAA artifacts, Athena results,
#   and dbt outputs in the same bucket are excluded.
# * Findings are exported to a dedicated S3 bucket (not the data lake) so they
#   survive even if the data lake bucket is recreated.
# * The job uses SCHEDULED sampling (not ONE_TIME) so it persists across applies.
# ─────────────────────────────────────────────────────────────────────────────

# ── Macie session ─────────────────────────────────────────────────────────────

resource "aws_macie2_account" "main" {
  finding_publishing_frequency = "FIFTEEN_MINUTES"
  status                       = "ENABLED"
}

# ── KMS key for Macie findings encryption ────────────────────────────────────

resource "aws_kms_key" "macie_findings" {
  description             = "Encrypts Macie PII-scan findings exported to S3"
  deletion_window_in_days = 7
  enable_key_rotation     = true

  tags = merge(local.tags, {
    Name = "${local.name_prefix}-macie-findings"
  })
}

resource "aws_kms_alias" "macie_findings" {
  name          = "alias/${local.name_prefix}-macie-findings"
  target_key_id = aws_kms_key.macie_findings.id
}

# ── Findings export bucket ────────────────────────────────────────────────────

resource "aws_s3_bucket" "macie_findings" {
  bucket = "${local.name_prefix}-macie-findings"

  tags = merge(local.tags, {
    Name = "${local.name_prefix}-macie-findings"
    Role = "macie-findings"
  })
}

resource "aws_s3_bucket_server_side_encryption_configuration" "macie_findings" {
  bucket = aws_s3_bucket.macie_findings.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "macie_findings" {
  bucket = aws_s3_bucket.macie_findings.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "macie_findings" {
  bucket = aws_s3_bucket.macie_findings.id

  rule {
    id     = "findings-expiry"
    status = "Enabled"

    filter {}

    expiration {
      days = 365
    }
  }
}

# Macie needs permission to write findings to this bucket.
resource "aws_s3_bucket_policy" "macie_findings" {
  bucket = aws_s3_bucket.macie_findings.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "MacieWriteFindings"
        Effect = "Allow"
        Principal = {
          Service = "macie.amazonaws.com"
        }
        Action = [
          "s3:PutObject",
        ]
        Resource = "${aws_s3_bucket.macie_findings.arn}/*"
        Condition = {
          StringEquals = {
            "aws:SourceAccount" = data.aws_caller_identity.current.account_id
          }
        }
      },
      {
        Sid    = "MacieGetBucketLocation"
        Effect = "Allow"
        Principal = {
          Service = "macie.amazonaws.com"
        }
        Action   = "s3:GetBucketLocation"
        Resource = aws_s3_bucket.macie_findings.arn
        Condition = {
          StringEquals = {
            "aws:SourceAccount" = data.aws_caller_identity.current.account_id
          }
        }
      },
    ]
  })

  depends_on = [aws_s3_bucket_public_access_block.macie_findings]
}

# ── Current account id (needed for bucket policy condition) ───────────────────

data "aws_caller_identity" "current" {}

# ── Monthly classification job ────────────────────────────────────────────────

resource "aws_macie2_classification_job" "data_lake" {
  name       = "${local.name_prefix}-data-lake-pii-scan"
  job_type   = "SCHEDULED"
  job_status = "RUNNING"

  schedule_frequency {
    monthly_schedule {
      day_of_month = 1
    }
  }

  s3_job_definition {
    bucket_definitions {
      account_id = data.aws_caller_identity.current.account_id
      buckets    = [aws_s3_bucket.data_lake.id]
    }

    # Scope the scan to the raw ingestion prefix only.
    scoping {
      includes {
        and {
          simple_scope_term {
            comparator = "STARTS_WITH"
            key        = "OBJECT_KEY"
            values     = ["comtrade/"]
          }
        }
      }
    }
  }

  depends_on = [aws_macie2_account.main]

  tags = local.tags
}

# ── Findings export configuration ────────────────────────────────────────────

resource "aws_macie2_findings_filter" "suppress_low_severity" {
  name        = "${local.name_prefix}-suppress-low-severity"
  description = "Suppress informational findings — trade statistics do not contain personal data"
  action      = "NOOP" # Change to ARCHIVE to auto-hide suppressed findings

  finding_criteria {
    criterion {
      field = "severity.description"
      eq    = ["Low"]
    }
  }
}

resource "aws_macie2_classification_export_configuration" "findings" {
  s3_destination {
    bucket_name = aws_s3_bucket.macie_findings.bucket
    key_prefix  = "findings/"
    kms_key_arn = aws_kms_key.macie_findings.arn
  }

  depends_on = [
    aws_macie2_account.main,
    aws_s3_bucket_policy.macie_findings,
  ]
}
