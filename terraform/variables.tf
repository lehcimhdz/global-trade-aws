variable "aws_region" {
  type        = string
  description = "AWS region for all resources"
  default     = "us-east-1"
}

variable "project_name" {
  type        = string
  description = "Project name — used as prefix for all resource names"
  default     = "global-trade"
}

variable "environment" {
  type        = string
  description = "Deployment environment"
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod"
  }
}

variable "data_lake_lifecycle_transition_days" {
  type        = number
  description = "Days before transitioning objects to GLACIER storage class"
  default     = 90
}

variable "data_lake_lifecycle_expiration_days" {
  type        = number
  description = "Days before permanently expiring objects (0 = never expire)"
  default     = 0
}

variable "secrets_recovery_window_days" {
  type        = number
  description = "Days to retain a deleted secret before permanent deletion (0 = immediate in non-prod)"
  default     = 0
}

# ── MWAA ──────────────────────────────────────────────────────────────────────

variable "enable_mwaa" {
  type        = bool
  description = "Deploy AWS MWAA (Managed Airflow). Set to false in dev (use Docker Compose instead)."
  default     = false
}

variable "mwaa_environment_class" {
  type        = string
  description = "MWAA environment class (mw1.small / mw1.medium / mw1.large)"
  default     = "mw1.small"
  validation {
    condition     = contains(["mw1.small", "mw1.medium", "mw1.large"], var.mwaa_environment_class)
    error_message = "mwaa_environment_class must be mw1.small, mw1.medium, or mw1.large"
  }
}

variable "mwaa_min_workers" {
  type        = number
  description = "Minimum number of MWAA workers"
  default     = 1
}

variable "mwaa_max_workers" {
  type        = number
  description = "Maximum number of MWAA workers"
  default     = 2
}

# ── QuickSight ─────────────────────────────────────────────────────────────────

variable "enable_quicksight" {
  type        = bool
  description = "Deploy QuickSight data source and datasets on top of the silver Iceberg tables."
  default     = false
}

variable "quicksight_username" {
  type        = string
  description = "QuickSight username (format: <username>) that will receive dataset permissions. Required when enable_quicksight = true."
  default     = ""
}
