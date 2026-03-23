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
