aws_region   = "us-east-1"
project_name = "global-trade"
environment  = "dev"

# Lifecycle: move to Glacier after 30 days in dev (cost control)
data_lake_lifecycle_transition_days = 30
data_lake_lifecycle_expiration_days = 180

# Secrets: delete immediately in dev (no recovery needed)
secrets_recovery_window_days = 0
