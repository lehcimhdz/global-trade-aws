# Changelog

All notable changes to this project are documented in this file. Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added
- **QuickSight monthly SPICE refresh** — `aws_quicksight_refresh_schedule` for both `reporter_summary` and `trade_flows`, fires on the 5th of every month (after the monthly `comtrade_dbt` rebuild). Timezone is configurable via `var.quicksight_refresh_timezone`.
- **Iceberg maintenance DAG** — `comtrade_iceberg_maintenance` runs `OPTIMIZE` (bin-pack) + `VACUUM` on every bronze and silver Iceberg table every Sunday 04:00 UTC, keeping snapshot count, manifest fan-out, and orphan-file storage flat as the lake grows.
- **`SPEC_IMPROVEMENTS.md`** — audit doc tracking the bugs and hardening items addressed in this iteration.

### Changed
- **API SQL parameterization** — `api/main.py` now passes user filters to Athena via `ExecutionParameters` (`?` placeholders) instead of interpolating them into f-strings, eliminating the SQLi-by-omission risk if a future filter skips the regex guard.
- **Lambda packaging** — `terraform/api.tf` no longer rebuilds `build/api.zip` via `archive_file` (which bundled only `api/*.py` and silently dropped the FastAPI/Mangum deps). It now references the make-built zip directly via `filebase64sha256`.
- **API runtime is async** — `api/athena.py::run_query` is now `async`; FastAPI endpoints are `async def`. Polling uses `asyncio.sleep` + `asyncio.to_thread` around boto3 (no new deps), so a warm Lambda container can serve other requests while a query polls.
- **Function URL auth is configurable** — new `var.api_function_url_auth_type` (NONE/AWS_IAM, default NONE) and `var.api_function_url_allowed_origins` (default `["*"]`) so production can flip the endpoint to SigV4 and tighten CORS without editing Terraform.
- **Silver dbt models are incremental** — `trade_flows` and `reporter_summary` now materialize as `incremental` + `merge` on their natural grain. Monthly runs only re-aggregate the latest period; `--full-refresh` remains available.
- **Terraform structural fixes** — added `local.tags` alias to `main.tf` (was referenced in 12 places but only `local.common_tags` existed); de-duplicated `data "aws_caller_identity" "current"` (was declared in both `macie.tf` and `quicksight.tf`).
- **Docs/cosmetic** — removed duplicate `── Trade API ──` header in the Makefile; README's "All DAGs share `retries=1`" claim corrected to reflect actual `retries=2` (ingestion) / `retries=1` (dbt + backfill).
- **Test count** — 633 → 667 unit tests with the new schedule, auth, maintenance DAG, async, and incremental coverage. 37 skips are Airflow-gated DAG tests when `apache-airflow` is not installed.
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
