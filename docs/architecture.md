# Architecture

## Overview

This system is a four-layer **ELT data platform** that ingests international trade statistics from the **UN Comtrade public API**, stores them in an **Apache Iceberg** data lake on AWS S3, transforms the data into queryable silver tables via **dbt + Amazon Athena**, and exposes it through a **FastAPI trade API** and **Amazon QuickSight** dashboards. The platform is orchestrated by **Apache Airflow 2.9** (local: Docker Compose · production: AWS MWAA) and fully provisioned with **Terraform**.

---

## High-level architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│  ① INGEST — Apache Airflow (Docker Compose / AWS MWAA)                 │
│                                                                         │
│  Webserver :8080  ◄── Scheduler ──► Redis (Celery broker)              │
│                              │                   │                      │
│                         PostgreSQL          Celery Workers              │
│                         (metadata)              │                       │
│                                                 │ HTTP + retry          │
│                              UN Comtrade Public API v1                  │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │ JSON responses (8 endpoints)
┌──────────────────────────────▼──────────────────────────────────────────┐
│  ② BRONZE — S3 + Apache Iceberg (Glue Data Catalog)                    │
│                                                                         │
│  s3://<bucket>/comtrade/*/           Raw JSON (Hive-partitioned)       │
│  s3://<bucket>/comtrade/*/parquet/   Columnar Parquet (optional)       │
│  s3://<bucket>/comtrade/iceberg/     Iceberg data + metadata files     │
│  s3://<bucket>/comtrade/errors/      Dead-letter manifests (90d)       │
│                                                                         │
│  Glue catalog: database=comtrade, one table per endpoint               │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │ Iceberg bronze tables
┌──────────────────────────────▼──────────────────────────────────────────┐
│  ③ SILVER — dbt-athena + Amazon Athena                                  │
│                                                                         │
│  comtrade_dbt DAG (@monthly):                                           │
│    dbt_deps → dbt_source_freshness → dbt_run_staging                   │
│             → dbt_run_silver → dbt_test                                 │
│                                                                         │
│  Staging layer (Athena views):  stg_preview · stg_mbs                  │
│  Silver layer (Iceberg tables): trade_flows · reporter_summary          │
│                                                                         │
│  Workgroup: comtrade (10 GB scan limit · CloudWatch metrics enabled)   │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │ Silver Iceberg tables
┌──────────────────────────────▼──────────────────────────────────────────┐
│  ④ SERVE                                                                │
│                                                                         │
│  Trade API (FastAPI + Mangum → Lambda Function URL)                    │
│    GET /v1/reporters              — list all reporter countries         │
│    GET /v1/reporters/{iso}/summary — country-level trade summary        │
│    GET /v1/trade-flows            — bilateral commodity trade flows     │
│                                                                         │
│  QuickSight (SPICE import mode)                                         │
│    Dataset: reporter_summary      — annual country totals               │
│    Dataset: trade_flows           — bilateral commodity flows           │
└─────────────────────────────────────────────────────────────────────────┘

Cross-cutting services
────────────────────────────────────────────────────────────────────────
  AWS Secrets Manager     — 9 Airflow Variables backed by Secrets Manager
  CloudWatch              — 7-widget dashboard + Athena BytesScanned alarm
  OpenLineage / Marquez   — dataset-to-dataset lineage across all tasks
  Slack                   — task failure + SLA miss notifications
  Lake Formation          — column-level access control on silver tables
  Amazon Macie            — monthly PII scan of the data lake bucket
```

---

## Component responsibilities

### Airflow Scheduler
Parses DAG files on a cycle, evaluates which DAG runs are due, and enqueues task instances into the Celery broker (Redis).

### Airflow Worker
Picks task instances from the Redis queue and executes them. Workers can be scaled horizontally.

### `plugins/comtrade/` package
Shared Python library mounted into every Airflow container. Contains:

| Module | Responsibility |
|--------|----------------|
| `client.py` | HTTP calls to the Comtrade API — rate limiting, retry with exponential backoff |
| `s3_writer.py` | S3 upload (JSON + Parquet), Hive-partitioned key construction |
| `dag_factory.py` | `@task` factory functions shared across all 8 ingestion DAGs |
| `validator.py` | Pure-Python 7-check data quality suite — no Airflow dependency |
| `callbacks.py` | Slack Block Kit alerts, SLA miss notifications, dead-letter S3 manifests |
| `metrics.py` | CloudWatch custom metrics — ingestion validation results + dbt run telemetry |
| `iceberg.py` | PyIceberg ACID writes to Glue-backed Iceberg tables with schema evolution |
| `lineage.py` | OpenLineage event emission to Marquez after each task |
| `schema.py` | Schema drift detection — column-set diff against per-endpoint S3 baseline |

### DAG files (`dags/`)
Thin orchestration files — one per endpoint for ingestion, plus `comtrade_dbt` for the silver layer and `comtrade_backfill` for historical loads.

### `api/` package
FastAPI application deployed as AWS Lambda via Mangum. Serves the silver Iceberg tables over HTTP.

| Module | Responsibility |
|--------|----------------|
| `main.py` | FastAPI routes — `/health`, `/v1/reporters`, `/v1/reporters/{iso}/summary`, `/v1/trade-flows` |
| `athena.py` | Synchronous Athena query runner — start, poll, paginate results |

### dbt project (`dbt/`)
dbt-athena-community project that reads from the Glue-registered Iceberg bronze tables and materialises two silver Iceberg tables via Amazon Athena.

| Model | Type | Description |
|-------|------|-------------|
| `stg_preview` | View | Renamed + typed columns from the `/preview` Iceberg table |
| `stg_mbs` | View | Monthly Bulletin of Statistics data |
| `trade_flows` | Iceberg table | Bilateral commodity-level aggregations, partitioned by `period` |
| `reporter_summary` | Iceberg table | Per-country export/import/balance totals, partitioned by `period` |

---

## Execution model

### Ingestion pipeline (8 DAGs, run in parallel)

Each DAG run produces three sequential tasks:

```
extract_and_store_raw  ──►  validate_bronze  ──►  convert_to_parquet
```

After `extract_and_store_raw`, a PyIceberg ACID append lands the records in the Glue-registered bronze Iceberg table.

After `validate_bronze`, four side-effects fire automatically (never masking task failures):
- **CloudWatch metrics** — `RowCount`, `ChecksPassed`, `ChecksFailed`, `JsonBytesWritten` emitted to `Comtrade/Pipeline` namespace.
- **Schema drift** — column set compared against the S3-persisted baseline; Slack alert on change.
- **Dead-letter** — on failure, an error manifest JSON is written to `s3://.../comtrade/errors/` (Hive-partitioned for Athena query).
- **OpenLineage** — run event emitted to Marquez when `OPENLINEAGE_URL` is configured.

### Transformation pipeline (`comtrade_dbt` DAG, monthly)

```
dbt_deps
    │
    ▼
dbt_source_freshness   ← blocks downstream if bronze data > 35 days old
    │
    ▼
dbt_run_staging    ── emit DbtRunDuration, DbtModelsErrored ──► CloudWatch
    │
    ▼
dbt_run_silver     ── emit DbtRunDuration, DbtModelsErrored ──► CloudWatch
    │
    ▼
dbt_test           ── emit DbtRunDuration, DbtTestsFailed   ──► CloudWatch
```

The three instrumented tasks use `PythonOperator` with `--log-format json` to parse dbt structured output and emit metrics before raising on failure.

### Backfill pipeline (`comtrade_backfill` DAG, manual)

`schedule=None`. Triggered via the Airflow UI or `make backfill ENDPOINT=preview PERIODS=2020,2021,2022`. Supports `preview`, `previewTariffline`, and `getMBS` endpoints.

---

## Infrastructure topology

### Development (local)

```
docker-compose.yml
├── postgres          ◄── metadata store
├── redis             ◄── Celery broker
├── airflow-init      (one-shot DB migration + admin user)
├── airflow-webserver
├── airflow-scheduler
├── airflow-worker
└── airflow-triggerer

Volumes:
  ./dags      → /opt/airflow/dags
  ./plugins   → /opt/airflow/plugins
  ./dbt       → /opt/airflow/dbt
  ./logs      → /opt/airflow/logs
```

### Production (AWS)

```
AWS VPC (10.0.0.0/16)
├── Public subnets  × 2   — Internet Gateway · NAT Gateway
└── Private subnets × 2   — MWAA workers · security groups

AWS MWAA (Managed Airflow)
├── Execution role        — Airflow data lake policy attached
├── Artifacts bucket (S3) — dags/ · plugins.zip · requirements.txt
└── Logging → CloudWatch Logs

Data plane
├── S3 data lake          — bronze JSON · Parquet · Iceberg metadata
├── Glue Data Catalog     — comtrade database → Iceberg tables
├── Athena workgroup      — comtrade (10 GB limit · CW metrics)
├── Lambda Function URL   — FastAPI trade API
└── QuickSight            — SPICE datasets on silver tables

Governance
├── Lake Formation        — data location · LF tags · column-level grants
├── Macie                 — monthly classification job on comtrade/ prefix
└── Secrets Manager       — 9 Airflow Variables

Observability
├── CloudWatch Dashboard  — 7 widgets (ingestion + dbt + errors)
├── CloudWatch Alarm      — Athena BytesScanned > 5 GB / 5 min
└── ECR                   — versioned Airflow Docker images
```

---

## Technology stack

| Layer | Technology | Version |
|-------|-----------|---------|
| Orchestration | Apache Airflow | 2.9.3 |
| Executor | CeleryExecutor | — |
| Message broker | Redis | 7.2 |
| Metadata store | PostgreSQL | 15 |
| Container runtime | Docker Compose (dev) / AWS MWAA (prod) | v2 / — |
| API client | requests + urllib3 Retry | 2.32.3 |
| Cloud storage | boto3 / AWS S3 | 1.34.144 |
| Columnar format | pandas + pyarrow | 2.2.2 / 16.1.0 |
| Table format | Apache Iceberg (PyIceberg) | 0.7.1 |
| Iceberg catalog | AWS Glue Data Catalog | — |
| SQL transformations | dbt-athena-community | 1.8.4 |
| Query engine | Amazon Athena (dedicated workgroup) | — |
| Base image | apache/airflow:2.9.3-python3.11 | — |
| Observability — metrics | AWS CloudWatch | — |
| Observability — lineage | Marquez (OpenLineage) | 0.50.0 |
| Data serving — API | FastAPI + Mangum (Lambda) | 0.111.1 / 0.17.0 |
| Data serving — BI | Amazon QuickSight (SPICE) | — |
| Access control | AWS Lake Formation | — |
| PII scanning | Amazon Macie | — |
| IaC | Terraform | ≥ 1.7 |
| CI / CD | GitHub Actions | — |
