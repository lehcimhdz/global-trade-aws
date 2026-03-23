# Architecture

## Overview

This system is an EL (Extract–Load) data pipeline that ingests international trade data from the **UN Comtrade public API** and stores it in **AWS S3** as a partitioned data lake. The pipeline is orchestrated by **Apache Airflow 2.9** running on a Docker Compose stack with the CeleryExecutor.

---

## High-level architecture

```
┌──────────────────────────────────────────────────────────────┐
│                        Docker Host                           │
│                                                              │
│  ┌────────────┐   ┌──────────────┐   ┌───────────────────┐  │
│  │ Airflow    │   │  Airflow     │   │  Airflow          │  │
│  │ Webserver  │   │  Scheduler   │   │  Worker(s)        │  │
│  │ :8080      │   │              │   │  (CeleryExecutor) │  │
│  └─────┬──────┘   └──────┬───────┘   └────────┬──────────┘  │
│        │                 │                    │              │
│        └────────┬─────────┘                   │              │
│                 │                             │              │
│          ┌──────▼──────┐   ┌─────────────────▼────────────┐ │
│          │  PostgreSQL  │   │           Redis               │ │
│          │  (metadata)  │   │  (Celery broker & backend)   │ │
│          └─────────────┘   └─────────────────────────────┘  │
│                                                              │
└─────────────────────────┬────────────────────────────────────┘
                          │ HTTP / HTTPS
          ┌───────────────▼──────────────┐
          │   UN Comtrade Public API     │
          │  comtradeapi.un.org/public   │
          └───────────────┬──────────────┘
                          │ JSON responses
          ┌───────────────▼──────────────┐
          │        AWS S3 Data Lake      │
          │  s3://<bucket>/comtrade/...  │
          └──────────────────────────────┘
```

---

## Component responsibilities

### Airflow Scheduler
Parses DAG files on a cycle, evaluates which DAG runs are due, and enqueues task instances into the Celery broker (Redis).

### Airflow Worker
Picks task instances from the Redis queue and executes them. Each task is a Python function decorated with `@task`. Workers can be scaled horizontally.

### Airflow Webserver
Provides the UI at `http://localhost:8080` for monitoring DAG runs, inspecting task logs, managing Variables and Connections, and manually triggering runs.

### PostgreSQL
Stores all Airflow metadata: DAG definitions, task states, run history, Variables, and Connections.

### Redis
Acts as the Celery message broker. The scheduler publishes task instances; workers consume them.

### `plugins/comtrade/` package
Shared Python library mounted into every Airflow container via the `plugins/` volume. Contains:

| Module | Responsibility |
|--------|----------------|
| `client.py` | HTTP calls to the Comtrade API, retry logic, rate limiting |
| `s3_writer.py` | S3 upload (JSON and Parquet), S3 key construction |
| `dag_factory.py` | `@task` factory functions shared across all DAGs |
| `validator.py` | Pure-Python data quality checks (7-check suite) |
| `callbacks.py` | Slack alerts, SLA miss notifications, dead-letter S3 manifests |
| `metrics.py` | CloudWatch custom metric emission after each validation run |
| `lineage.py` | OpenLineage event emission to Marquez after each task |
| `schema.py` | Schema drift detection — column-set diff against S3 baseline + Slack alert |
| `iceberg.py` | Apache Iceberg writer — ACID appends to Glue-backed tables with schema evolution |

### DAG files (`dags/`)
Thin orchestration files — one per API endpoint. They declare schedules, read Airflow Variables, and wire tasks produced by the factory.

### `api/` package
FastAPI application deployed as AWS Lambda. Serves the silver Iceberg tables to external consumers via HTTP.

| Module | Responsibility |
|--------|----------------|
| `main.py` | FastAPI routes — `/health`, `/v1/reporters`, `/v1/reporters/{iso}/summary`, `/v1/trade-flows` |
| `athena.py` | Synchronous Athena query runner — start, poll, paginate results into list of dicts |

---

## Execution model

Each DAG run produces **three sequential tasks**:

```
extract_and_store_raw  ──►  validate_bronze  ──►  convert_to_parquet
       (always)                (always)              (conditional)
```

`convert_to_parquet` is a no-op when `COMTRADE_WRITE_PARQUET` is not `"true"`.

After each task, cross-cutting side effects fire automatically (never masking task failures):
- **Schema drift** — `validate_bronze` compares column sets against a per-endpoint S3 baseline; alerts on change.
- **CloudWatch metrics** — emitted by `validate_bronze` (row count, check pass/fail, bytes written).
- **OpenLineage events** — emitted by every task to Marquez when `OPENLINEAGE_URL` is configured.

The **`comtrade_dbt`** DAG orchestrates a four-task dbt run (`dbt_deps → dbt_run_staging → dbt_run_silver → dbt_test`) that transforms the Iceberg bronze tables into queryable silver tables via Amazon Athena.  It is scheduled monthly, after the ingestion window.

---

## Infrastructure topology

```
┌─────────────────────────────────────────────────────┐
│  docker-compose.yml                                 │
│                                                     │
│   airflow-init ──► (one-shot DB migration + user)  │
│                                                     │
│   postgres      ◄── airflow-webserver               │
│       ▲              airflow-scheduler              │
│       │              airflow-worker                 │
│   redis         ◄──  airflow-triggerer              │
│                                                     │
│  Volumes:                                           │
│    ./dags     → /opt/airflow/dags                   │
│    ./plugins  → /opt/airflow/plugins                │
│    ./logs     → /opt/airflow/logs                   │
│    ./config/airflow_variables.json (read-only)      │
│    postgres-db-volume (named, persisted)            │
└─────────────────────────────────────────────────────┘
```

---

## Technology stack

| Layer | Technology | Version |
|-------|-----------|---------|
| Orchestration | Apache Airflow | 2.9.3 |
| Executor | CeleryExecutor | — |
| Message broker | Redis | 7.2 |
| Metadata store | PostgreSQL | 15 |
| Container runtime | Docker Compose | v2 |
| API client | requests + urllib3 Retry | 2.32.3 |
| Cloud storage | boto3 / AWS S3 | 1.34.144 |
| Columnar format | pandas + pyarrow | 2.2.2 / 16.1.0 |
| Table format | Apache Iceberg (PyIceberg) | 0.7.1 |
| Iceberg catalog | AWS Glue Data Catalog | — |
| SQL transformations | dbt (dbt-athena-community) | 1.8.4 |
| Query engine | Amazon Athena (dedicated workgroup) | — |
| Base image | apache/airflow:2.9.3-python3.11 | — |
| Observability — metrics | AWS CloudWatch | — |
| Observability — lineage | Marquez (OpenLineage) | 0.50.0 |
| Data serving — API | FastAPI + Mangum (Lambda) | 0.111.1 / 0.17.0 |
| Data serving — BI | Amazon QuickSight (SPICE) | — |
