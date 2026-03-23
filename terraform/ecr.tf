# ── ECR — Container image registry ───────────────────────────────────────────
#
# Stores the custom Airflow Docker image used by:
#   - Local / Docker Compose development (replaces upstream image when customised)
#   - Future ECS-based deployment as an alternative to MWAA
#
# The CI deploy workflow builds the image from Dockerfile, tags it with the
# Git SHA, and pushes it here on every merge to main.
# ─────────────────────────────────────────────────────────────────────────────

resource "aws_ecr_repository" "airflow" {
  name                 = "${local.name_prefix}-airflow"
  image_tag_mutability = "IMMUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = {
    Name = "${local.name_prefix}-airflow"
    Role = "container-registry"
  }
}

resource "aws_ecr_lifecycle_policy" "airflow" {
  repository = aws_ecr_repository.airflow.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Keep the 20 most recent tagged images"
        selection = {
          tagStatus     = "tagged"
          tagPrefixList = ["sha-"]
          countType     = "imageCountMoreThan"
          countNumber   = 20
        }
        action = { type = "expire" }
      },
      {
        rulePriority = 2
        description  = "Expire untagged images after 14 days"
        selection = {
          tagStatus   = "untagged"
          countType   = "sinceImagePushed"
          countUnit   = "days"
          countNumber = 14
        }
        action = { type = "expire" }
      },
    ]
  })
}

# ── IAM: allow MWAA execution role to pull from ECR ──────────────────────────
# Attach only when MWAA is enabled.

resource "aws_iam_role_policy" "mwaa_ecr" {
  count = var.enable_mwaa ? 1 : 0

  name = "${local.name_prefix}-mwaa-ecr-pull"
  role = aws_iam_role.mwaa[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ECRPull"
        Effect = "Allow"
        Action = [
          "ecr:GetDownloadUrlForLayer",
          "ecr:BatchGetImage",
          "ecr:BatchCheckLayerAvailability",
          "ecr:GetAuthorizationToken",
        ]
        Resource = "*"
      }
    ]
  })
}
