output "data_lake_bucket_name" {
  value       = aws_s3_bucket.data_lake.id
  description = "S3 bucket name — set as COMTRADE_S3_BUCKET in Airflow Variables"
}

output "data_lake_bucket_arn" {
  value       = aws_s3_bucket.data_lake.arn
  description = "S3 bucket ARN"
}

output "data_lake_bucket_region" {
  value       = aws_s3_bucket.data_lake.region
  description = "Region where the data lake bucket was created"
}

output "airflow_role_arn" {
  value       = aws_iam_role.airflow.arn
  description = "IAM role ARN — attach to EC2 instances or ECS task definitions running Airflow"
}

output "airflow_policy_arn" {
  value       = aws_iam_policy.airflow_data_lake.arn
  description = "IAM policy ARN — attach directly to any principal that needs S3 + Secrets Manager access"
}

output "airflow_dev_user_name" {
  value       = var.environment == "dev" ? aws_iam_user.airflow_dev[0].name : null
  description = "IAM username for local development (dev environment only)"
}

output "secrets_manager_paths" {
  value = {
    for k, v in aws_secretsmanager_secret.airflow_variable : k => v.name
  }
  description = "Map of variable name → Secrets Manager path for all managed Airflow Variables"
}

output "ecr_repository_url" {
  value       = aws_ecr_repository.airflow.repository_url
  description = "ECR repository URL — use as base image in Dockerfile and for docker push"
}

output "mwaa_webserver_url" {
  value       = var.enable_mwaa ? aws_mwaa_environment.main[0].webserver_url : null
  description = "MWAA Airflow UI URL (null when enable_mwaa = false)"
}

output "mwaa_artifacts_bucket" {
  value       = var.enable_mwaa ? aws_s3_bucket.mwaa_artifacts[0].id : null
  description = "S3 bucket holding DAGs, plugins.zip, and requirements.txt for MWAA"
}

output "athena_workgroup_name" {
  value       = aws_athena_workgroup.comtrade.name
  description = "Athena workgroup name — set as COMTRADE_DBT_TARGET workgroup in dbt profiles.yml"
}

output "athena_named_queries" {
  value = {
    top_exporters_by_period    = aws_athena_named_query.top_exporters_by_period.id
    top_commodities_by_reporter = aws_athena_named_query.top_commodities_by_reporter.id
    bilateral_trade_balance    = aws_athena_named_query.bilateral_trade_balance.id
    yoy_growth_by_reporter     = aws_athena_named_query.yoy_growth_by_reporter.id
    data_freshness_check       = aws_athena_named_query.data_freshness_check.id
  }
  description = "Map of named query identifiers — run via the Athena console or API"
}

output "cloudwatch_dashboard_url" {
  value       = "https://${var.aws_region}.console.aws.amazon.com/cloudwatch/home?region=${var.aws_region}#dashboards:name=${aws_cloudwatch_dashboard.comtrade_pipeline.dashboard_name}"
  description = "Direct link to the Comtrade Pipeline CloudWatch dashboard"
}

output "next_steps" {
  value = <<-EOT
    Infrastructure created. Next steps:

    1. Create IAM access keys for the dev user in the AWS Console:
       IAM → Users → ${local.name_prefix}-airflow-dev → Security credentials → Access keys

    2. Populate Secrets Manager with real values:
       make bootstrap-secrets ENV=${var.environment}

    3. Set COMTRADE_S3_BUCKET in your .env:
       echo 'COMTRADE_S3_BUCKET=${aws_s3_bucket.data_lake.id}' >> .env

    4. Start Airflow:
       make init && make up
  EOT
  description = "Post-apply checklist"
}
