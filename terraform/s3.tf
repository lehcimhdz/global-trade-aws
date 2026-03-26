# ── Data lake bucket ──────────────────────────────────────────────────────────

resource "aws_s3_bucket" "data_lake" {
  bucket = "${local.name_prefix}-data-lake"

  tags = {
    Name = "${local.name_prefix}-data-lake"
    Role = "data-lake"
  }
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  # Depends on versioning being enabled first
  depends_on = [aws_s3_bucket_versioning.data_lake]

  bucket = aws_s3_bucket.data_lake.id

  # Transition raw JSON (cheaper than Parquet to query — archive faster)
  rule {
    id     = "raw-json-lifecycle"
    status = "Enabled"

    filter {
      and {
        prefix = "comtrade/"
        tags = {
          fmt = "json"
        }
      }
    }

    transition {
      days          = var.data_lake_lifecycle_transition_days
      storage_class = "GLACIER_IR" # Instant Retrieval — re-query within ms
    }

    dynamic "expiration" {
      for_each = var.data_lake_lifecycle_expiration_days > 0 ? [1] : []
      content {
        days = var.data_lake_lifecycle_expiration_days
      }
    }

    noncurrent_version_transition {
      noncurrent_days = 30
      storage_class   = "GLACIER"
    }

    noncurrent_version_expiration {
      noncurrent_days = 90
    }
  }

  # Keep Parquet warm longer — it's queried by Athena
  rule {
    id     = "parquet-lifecycle"
    status = "Enabled"

    filter {
      prefix = "comtrade/*/fmt=parquet/"
    }

    transition {
      days          = var.data_lake_lifecycle_transition_days * 2
      storage_class = "INTELLIGENT_TIERING"
    }

    noncurrent_version_expiration {
      noncurrent_days = 90
    }
  }

  # Expire dead-letter manifests after 90 days — they exist for post-mortem
  # debugging only; the canonical failure record is the Airflow task log.
  rule {
    id     = "errors-expiry"
    status = "Enabled"

    filter {
      prefix = "comtrade/errors/"
    }

    expiration {
      days = 90
    }

    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }

  # Athena query results are ephemeral — expire after 30 days.
  rule {
    id     = "athena-results-expiry"
    status = "Enabled"

    filter {
      prefix = "athena-results/"
    }

    expiration {
      days = 30
    }
  }

  # Iceberg snapshot metadata files under dbt/silver/ and comtrade/iceberg/.
  # S3 lifecycle removes orphaned metadata files after the configured retention
  # window.  Full snapshot expiry (manifest cleanup) still requires running
  # `VACUUM` via Athena or `expire_snapshots()` via PyIceberg.
  dynamic "rule" {
    for_each = var.iceberg_snapshot_retention_days > 0 ? [1] : []
    content {
      id     = "iceberg-metadata-expiry"
      status = "Enabled"

      filter {
        and {
          prefix = ""
          tags = {
            iceberg = "metadata"
          }
        }
      }

      noncurrent_version_expiration {
        noncurrent_days = var.iceberg_snapshot_retention_days
      }
    }
  }

  # Always clean up incomplete multipart uploads
  rule {
    id     = "abort-incomplete-multipart"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

resource "aws_s3_bucket_notification" "data_lake" {
  # Placeholder — wire to EventBridge when adding Glue Crawlers in Tier 2
  bucket = aws_s3_bucket.data_lake.id
}
