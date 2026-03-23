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

locals {
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
    Repository  = "global-trade-aws"
  }

  name_prefix = "${var.project_name}-${var.environment}"
}
