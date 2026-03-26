# Testing Guide

## Overview

The test suite is split into three tiers:

| Tier | Location | Airflow required | Count |
|------|----------|-----------------|-------|
| **Unit** | `tests/unit/` | Only for DAG factory / callbacks / backfill | 633 pass · 3 skip |
| **DAG integrity** | `tests/dag_integrity/` | Yes | All 10 DAGs (8 ingestion + `comtrade_dbt` + `comtrade_backfill`) |
| **Integration smoke** | `tests/integration/` | Yes | 32 tests — full bronze pipeline against moto S3 |

Tests that depend on Airflow are automatically **skipped** (not failed) when `apache-airflow` is not installed. No business-logic module (`validator.py`, `client.py`, `metrics.py`, etc.) imports Airflow at module level.

---

## Setup

### Install test dependencies (no Airflow)

```bash
pip install -r tests/requirements-test.txt
```

### Install Airflow (to unlock all tests)

```bash
pip install apache-airflow==2.9.3 apache-airflow-providers-amazon==8.24.0
```

Or use the Makefile:

```bash
make install-dev   # installs everything including pre-commit
```

---

## Running tests

```bash
make test              # unit tests only (no Airflow, ~1 s)
make test-full         # full suite: unit + DAG integrity + integration
make test-integration  # integration smoke tests only
make test-cov          # full suite with HTML coverage report
make test-api          # trade API unit tests only
```

### By path

```bash
pytest tests/unit/ -v
pytest tests/dag_integrity/ -v
pytest tests/integration/ -v
```

### By marker

```bash
pytest -m unit
pytest -m dag_integrity
pytest -m integration
```

### With coverage

```bash
pytest --cov=comtrade --cov-report=term-missing
```

---

## Test structure

```
tests/
├── conftest.py                     sys.path, env vars, sample_api_response fixture
├── unit/
│   ├── conftest.py                 mock_variables fixture, Airflow DB init
│   │
│   │── Business logic ────────────────────────────────────────────
│   ├── test_client.py              HTTP client (no Airflow)
│   ├── test_s3_writer.py           S3 key builder + upload helpers (moto)
│   ├── test_dag_factory.py         extract / validate / parquet task logic
│   ├── test_validator.py           7-check quality suite
│   ├── test_callbacks.py           Slack alerts, dead-letter, SLA miss
│   ├── test_metrics.py             Ingestion + dbt CloudWatch metrics (42 tests)
│   ├── test_iceberg.py             Iceberg writer (sys.modules mock for pyiceberg)
│   ├── test_lineage.py             OpenLineage event builder
│   ├── test_schema.py              Schema drift detection
│   ├── test_dbt_project.py         dbt YAML structure + SQL coverage (no dbt needed)
│   ├── test_backfill_dag.py        Backfill DAG factory (Airflow-gated)
│   │
│   │── API layer ─────────────────────────────────────────────────
│   ├── test_api.py                 FastAPI endpoints (89 tests)
│   │
│   │── Terraform structural checks ────────────────────────────────
│   ├── test_api_terraform.py       Lambda + Function URL config
│   ├── test_athena_terraform.py    Workgroup + 5 named queries
│   ├── test_quicksight_terraform.py SPICE datasets + IAM role (76 tests)
│   ├── test_cloudwatch_terraform.py Dashboard widgets + Athena alarm
│   ├── test_lake_formation_terraform.py LF tags + per-role permissions
│   └── test_macie_terraform.py     Macie session + job + findings export
│
├── dag_integrity/
│   └── test_dag_integrity.py       DagBag tests for all 10 DAGs
└── integration/
    └── test_pipeline_smoke.py      32 multi-component tests (moto S3)
```

---

## What is tested

### `test_client.py`

| Class | What is verified |
|-------|-----------------|
| `TestInternalGet` | `None` params stripped; rate-limit sleep called; HTTP errors raise; JSON returned |
| `TestGetPreview` | Correct URL; all optional params forwarded; defaults to `None` |
| `TestGetPreviewTariffline` | Extra params (`partner2Code`, `customsCode`, `motCode`) forwarded |
| `TestGetWorldShare` | Correct URL (no `clCode`); period/reporter forwarded |
| `TestGetMetadata` | Correct URL; no query params emitted |
| `TestGetMBS` | Flat URL; `fmt` → `format` key; all optional |
| `TestGetDATariffline` | Correct URL; no query params |
| `TestGetDA` | Correct URL |
| `TestGetComtradeReleases` | Correct URL; empty params |
| `TestRetryBehaviour` | Succeeds after transient 5xx; raises after retries exhausted |

### `test_metrics.py` (42 tests)

| Class | What is verified |
|-------|-----------------|
| `TestBuildMetricData` | Correct metric names, values, dimensions for ingestion metrics |
| `TestEmitValidationMetrics` | `put_metric_data` called; namespace correct; error isolation; region fallback |
| `TestBuildDbtMetricData` | `DbtRunDuration` / `DbtModelsErrored` / `DbtTestsFailed` values + units; `DagId` + `Phase` dimensions |
| `TestEmitDbtMetrics` | `put_metric_data` called; namespace correct; defaults to zero errors/failures; error isolation; log messages |

### `test_api.py` (89 tests)

| Class | What is verified |
|-------|-----------------|
| `TestHealth` | 200 + `{"status": "ok"}` |
| `TestGetReporters` | Valid period; missing period → 422; Athena result mapped to schema |
| `TestGetReporterSummary` | ISO path param; not-found → 404; balance + value fields present |
| `TestGetTradeFlows` | Required params; pagination; flow_code filter; commodity filter |

### Terraform structural tests

These tests parse `.tf` files as text — no Terraform binary or AWS credentials required. They catch typos, missing resources, and malformed configuration before CI runs `terraform validate`.

| File | Resources verified |
|------|-------------------|
| `test_athena_terraform.py` | Workgroup enforcement, CW metrics, scan cutoff, 5 named queries, SQL correctness |
| `test_quicksight_terraform.py` | IAM role, Athena data source, 2 SPICE datasets, column mappings (76 tests) |
| `test_api_terraform.py` | Lambda function URL, IAM role, Athena/S3/Glue permissions |
| `test_cloudwatch_terraform.py` | 7 dashboard widgets (names, metrics, dimensions), Athena alarm config |
| `test_lake_formation_terraform.py` | LF tags, database association, per-role SELECT/ALTER, column-level grant for API Lambda |
| `test_macie_terraform.py` | Macie account enabled, KMS key with rotation, findings bucket lifecycle, monthly job scope, export config |

### `test_dag_integrity.py`

| Class | What is verified |
|-------|-----------------|
| `TestDagBagLoads` | Zero import errors; all 10 DAGs present |
| `TestDagSchedule` | Each DAG's schedule matches expected value |
| `TestDagConfig` | `catchup=False`; tags; description; `retries ≥ 1`; `start_date` set |
| `TestDagTasks` | Correct task count; correct task IDs; dependency chain; no cycles |
| `TestDbtDag` | 5 tasks; `deps → freshness → staging → silver → test` chain |
| `TestBackfillDag` | `schedule=None`; `validate_conf → run_backfill` chain |

### `test_pipeline_smoke.py` (32 tests)

| Class | What is verified |
|-------|-----------------|
| `TestBronzeWriteAndRead` | Hive key; object exists; Content-Type; round-trip fidelity; Unicode |
| `TestValidationPipeline` | All 7 checks pass on valid data; bad envelope / empty data fail |
| `TestParquetPipeline` | Parquet written; `fmt=parquet/` in key; column names + values preserved |
| `TestSchemaDriftPipeline` | Baseline saved; same schema → no drift; added/removed column → drift |
| `TestDeadLetterPipeline` | Manifest exists; Hive key; valid JSON; required fields + exception message |
| `TestFullPipelineChain` | extract → validate → parquet sequence; schema drift across 3 runs |

---

## Mocking strategy

| Target | Strategy |
|--------|---------|
| HTTP (client tests) | `responses` library intercepts `requests.Session.get` — no real network calls |
| AWS S3 | `moto` (`mock_aws`) intercepts all `boto3` calls with an in-memory S3 service |
| AWS CloudWatch | `mock.patch("boto3.client")` returns a `MagicMock` client |
| Airflow Variables | `airflow.models.Variable.get` replaced by a plain dict via `monkeypatch` |
| Airflow `@task` | Accessed via `.function` attribute (TaskFlow API) — no scheduler/DB required |
| PyIceberg | Injected via `sys.modules` — no real Glue catalog calls |
| Airflow DB (integrity tests) | `airflow.utils.db.initdb()` creates a SQLite DB at `tests/.airflow_test.db` once per session |

Integration tests combine moto S3 + monkeypatched Variables — no S3 calls mocked at function level, so the full serialization/deserialization path runs.

---

## CI integration

The existing `ci.yml` runs three test jobs:

| Job | Command | Runs on |
|-----|---------|---------|
| `unit-tests` | `pytest tests/unit/ -v` | Every push |
| `full-tests` | `pytest -v` (matrix py3.10 + py3.11) | Every push |
| `terraform-validate` | `terraform init -backend=false && terraform validate` | Every push |

All jobs must pass before a PR can merge.
