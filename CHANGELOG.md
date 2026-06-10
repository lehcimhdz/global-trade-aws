# Changelog

All notable changes to this project are documented in this file. Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Changed
- README badges and clone URLs now point to the real GitHub org (`lehcimhdz`) instead of the `your-org` placeholder.
- README now documents the project's "portfolio showcase" status and that MWAA/Lambda/QuickSight components are gated behind Terraform variables so the stack does not incur standing AWS costs.

## Initial release (2026-03)

### Added
- Four-layer ELT platform: bronze (raw JSON + Iceberg), silver (dbt-athena), serve (FastAPI on Lambda + QuickSight SPICE).
- 8 ingestion DAGs covering UN Comtrade public API endpoints + `comtrade_dbt` and `comtrade_backfill`.
- Shared `comtrade` plugin package: HTTP client with rate limiting, S3 writer, dag factory, 7-check data validator, Slack callbacks, CloudWatch metrics, PyIceberg writer, OpenLineage events, schema drift detection.
- Terraform stack (16 files): S3 lake with 5 lifecycle rules, IAM, Glue, Athena workgroup with named queries, CloudWatch dashboard + Athena cost alarm, Lake Formation column-level ACLs, Macie monthly PII scan, Secrets Manager, MWAA, ECR, VPC, Lambda Function URL, QuickSight datasets.
- GitHub Actions CI (lint → unit tests → full tests → terraform validate) with `ci-pass` branch protection gate.
- Deploy workflow (ECR build → dev → staging → prod with manual approvals).
- 633 unit tests with no Airflow dependency for business logic, plus DAG integrity and moto integration tests.
- Docker Compose stack for local Airflow + Postgres + Redis.
- Makefile (40+ targets) and 8 documentation guides under `docs/`.
- MIT License.
