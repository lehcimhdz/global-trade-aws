terraform {
  required_version = ">= 1.7"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # ── Remote state (enable after creating the state bucket manually) ──────────
  # backend "s3" {
  #   bucket         = "<your-project>-terraform-state"
  #   key            = "global-trade-aws/${var.environment}/terraform.tfstate"
  #   region         = "us-east-1"
  #   encrypt        = true
  #   dynamodb_table = "<your-project>-terraform-locks"
  # }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = local.common_tags
  }
}

# Shared across macie.tf and quicksight.tf — declared once here to avoid
# duplicate data source declarations.
data "aws_caller_identity" "current" {}

locals {
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
    Repository  = "global-trade-aws"
  }

  # Alias for resources that reference `local.tags` (most of the .tf files
  # use this name for tagging individual resources beyond the provider's
  # default_tags block).
  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
    Repository  = "global-trade-aws"
  }

  name_prefix = "${var.project_name}-${var.environment}"
}
