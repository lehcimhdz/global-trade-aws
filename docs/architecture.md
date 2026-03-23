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

### DAG files (`dags/`)
Thin orchestration files — one per API endpoint. They declare schedules, read Airflow Variables, and wire tasks produced by the factory.

---

## Execution model

Each DAG run produces **two sequential tasks**:

```
extract_and_store_raw  ──►  convert_to_parquet
       (always)                (conditional)
```

The second task is a no-op when the `COMTRADE_WRITE_PARQUET` Variable is not set to `"true"`.

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
| Base image | apache/airflow:2.9.3-python3.11 | — |
