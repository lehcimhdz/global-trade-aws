aws_region   = "us-east-1"
project_name = "global-trade"
environment  = "prod"

# Lifecycle: keep data warm for 90 days; archive to Glacier IR after that
data_lake_lifecycle_transition_days = 90
data_lake_lifecycle_expiration_days = 0  # never expire production data

# Secrets: 30-day recovery window protects against accidental deletion
secrets_recovery_window_days = 30
