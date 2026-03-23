# Testing Guide

## Overview

The test suite is split into three tiers:

| Tier | Location | Airflow required | Coverage |
|------|----------|-----------------|----------|
| **Unit** | `tests/unit/` | Only for S3 / DAG factory / callbacks | API client, writers, validator, schema drift, Iceberg, metrics, lineage, callbacks, dbt project structure |
| **DAG integrity** | `tests/dag_integrity/` | Yes | All 9 DAGs (8 ingestion + `comtrade_dbt`) parse cleanly, correct structure and schedules |
| **Integration smoke** | `tests/integration/` | Yes | Multi-component end-to-end: bronze → validate → parquet → schema drift → dead-letter, against moto S3 |

Tests that depend on Airflow are automatically **skipped** (not failed) when
`apache-airflow` is not installed in the current environment. The pure-Python
API client tests and dbt project structure tests always run.

---

## Setup

### Install test dependencies

```bash
pip install -r tests/requirements-test.txt
```

### Install Airflow (to unlock all tests)

```bash
pip install apache-airflow==2.9.3 apache-airflow-providers-amazon==8.24.0
```

---

## Running tests

### Full suite

```bash
pytest
```

### Only the always-available client tests (no Airflow needed)

```bash
pytest tests/unit/test_client.py -v
```

### Only unit tests

```bash
pytest tests/unit/ -v
```

### Only DAG integrity tests

```bash
pytest tests/dag_integrity/ -v
```

### Only integration smoke tests

```bash
pytest tests/integration/ -v
# or with the make target:
make test-integration
```

### By marker

```bash
pytest -m unit            # pure unit tests
pytest -m dag_integrity   # structural DAG tests
pytest -m integration     # multi-component smoke tests
```

### With coverage

```bash
pytest --cov=comtrade --cov-report=term-missing
```

---

## Test structure

```
tests/
├── conftest.py                     # sys.path, env vars, sample_api_response fixture
├── requirements-test.txt           # test-only dependencies
├── unit/
│   ├── conftest.py                 # mock_variables fixture, Airflow DB init
│   ├── test_client.py              # HTTP client (no Airflow dependency)
│   ├── test_s3_writer.py           # S3 key builder + upload helpers (moto)
│   ├── test_dag_factory.py         # extract/validate/parquet task logic
│   ├── test_validator.py           # 7-check quality suite
│   ├── test_callbacks.py           # Slack alerts, dead-letter, SLA miss
│   ├── test_metrics.py             # CloudWatch metric builder
│   ├── test_lineage.py             # OpenLineage event builder
│   ├── test_schema.py              # Schema drift detection
│   ├── test_iceberg.py             # Iceberg writer (sys.modules mock for pyiceberg)
│   └── test_dbt_project.py         # dbt YAML structure + SQL coverage (no dbt needed)
├── dag_integrity/
│   ├── conftest.py                 # Airflow DB init for DAG loading
│   └── test_dag_integrity.py       # DagBag structure for all 9 DAGs
└── integration/
    ├── conftest.py                 # mock_variables + moto S3 bucket fixtures
    └── test_pipeline_smoke.py      # multi-component end-to-end tests
```

---

## What is tested

### `test_client.py` (24 tests)

| Class | What is verified |
|-------|-----------------|
| `TestInternalGet` | `None` params are stripped from query string; rate-limit sleep is called before every request; HTTP errors raise; JSON is returned |
| `TestGetPreview` | Correct URL built; all optional params forwarded; params default to `None` |
| `TestGetPreviewTariffline` | Extra params (`partner2Code`, `customsCode`, `motCode`) forwarded |
| `TestGetWorldShare` | Correct URL (no `clCode`); period/reporter forwarded |
| `TestGetMetadata` | Correct URL; no query params emitted |
| `TestGetMBS` | Correct flat URL; `fmt` mapped to `format` key; all optional |
| `TestGetDATariffline` | Correct URL; no query params |
| `TestGetDA` | Correct URL |
| `TestGetComtradeReleases` | Correct URL; empty params |
| `TestRetryBehaviour` | Succeeds after a transient 5xx; raises after retries exhausted |

### `test_s3_writer.py` (13 tests, requires Airflow + moto)

| Class | What is verified |
|-------|-----------------|
| `TestBuildS3Key` | Full key with type/freq; no-param endpoints; extra partitions; Parquet format adds `fmt=parquet/` prefix; `run_id` preserved; JSON is default format |
| `TestWriteJsonToS3` | Returns correct `s3://` URI; object is valid JSON; Content-Type is `application/json`; UTF-8 preserved; non-serialisable types handled via `default=str` |
| `TestWriteParquetToS3` | Returns correct URI; Parquet is readable with pandas; column values preserved; nested dicts normalised; single-record input works |

### `test_dag_factory.py` (20 tests, requires Airflow)

| Class | What is verified |
|-------|-----------------|
| `TestHelpers` | `_bucket()` reads Variable; `_parquet_enabled()` is false by default, true for `"true"` string, case-insensitive |
| `TestExtractTask` | API function is called with kwargs from `api_kwargs_fn`; returns S3 key string; key contains endpoint, year, month; `run_id` colons/plus-signs sanitised; correct bucket used; API response forwarded to writer; extra partitions in key |
| `TestParquetTask` | Returns `None` when disabled; no S3 read when disabled; reads JSON from correct S3 key; extracts `data[]` from envelope; returns `None` for empty data; returns `None` for non-list data; Parquet key contains `fmt=parquet`; bare list response used as-is |

### `test_dag_integrity.py` (requires Airflow)

| Class | What is verified |
|-------|-----------------|
| `TestDagBagLoads` | Zero import errors; all 9 DAGs present; no unexpected DAGs |
| `TestDagSchedule` | Each DAG's schedule matches expected value |
| `TestDagConfig` | `catchup=False` on all DAGs; tags match exactly; description non-empty; `retries >= 1`; `start_date` set |
| `TestDagTasks` | Correct task count per DAG; correct task IDs; dependency chain correct; no cycles |
| `TestDagFiles` | All 9 DAG files on disk; plugin `__init__.py` exists |
| `TestDbtDag` | `comtrade_dbt` has 4 tasks; deps → staging → silver → test dependency chain; no cycles |

### `test_pipeline_smoke.py` (32 tests, requires Airflow + moto)

| Class | What is verified |
|-------|-----------------|
| `TestBronzeWriteAndRead` | Hive-partitioned key; object exists; Content-Type; record count/values/columns preserved on round-trip; Unicode characters survive serialisation |
| `TestValidationPipeline` | All 7 checks pass on valid S3 data; `check_envelope` passes; `check_has_data_key` passes; `check_row_count` passes; bad envelope fails; empty data fails |
| `TestParquetPipeline` | Parquet written to S3; key has `fmt=parquet/`; column names preserved; numeric values preserved (approx) |
| `TestSchemaDriftPipeline` | First run saves baseline to S3; second run same schema → no drift; added column → drift detected; removed column → drift detected; baseline updated after drift; endpoints have independent baselines; empty records skips detection |
| `TestDeadLetterPipeline` | Manifest object exists in S3; key contains `dag_id=` and `task_id=`; manifest is valid JSON; required fields present; exception message captured |
| `TestFullPipelineChain` | Extract → validate → parquet in sequence; extract → schema drift across three runs |

---

## Mocking strategy

### HTTP (client tests)
The `responses` library intercepts `requests.Session.get` at the transport level — no real network calls are made.

### AWS S3 (s3_writer tests)
`moto` (`mock_aws` decorator) intercepts all `boto3` calls and simulates an in-memory S3 service. The test bucket is created inside each test using the mocked client.

### Airflow Variables (s3_writer, dag_factory tests)
`airflow.models.Variable.get` is replaced by a plain dict lookup via `monkeypatch`. The dict is returned from the `mock_variables` fixture and individual tests can mutate it to test different configurations.

### Airflow `@task` decorator (dag_factory tests)
The task factory returns Airflow `@task`-decorated objects. The underlying Python function is accessed via the `.function` attribute (standard Airflow 2.x TaskFlow API) and called directly with a synthetic context dict — no Airflow scheduler or DB required.

### Airflow DB (dag_integrity tests)
`airflow.utils.db.initdb()` creates a SQLite database at `tests/.airflow_test.db` once per session. The `DagBag` uses this DB to resolve imports.

### Integration tests (multi-component)
Integration tests combine all of the above — real `moto` S3 objects are written and read across multiple modules. No S3 calls are mocked at the function level; `mock_aws()` intercepts boto3 at the transport layer so the full serialization/deserialization path runs. Airflow Variables are patched via `monkeypatch`. `pyiceberg` is excluded from integration scope (covered by unit tests with `sys.modules` injection).

---

## CI integration

Add to `.github/workflows/ci.yml`:

```yaml
name: Tests
on: [push, pull_request]

jobs:
  unit:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install -r tests/requirements-test.txt
      - run: pytest tests/unit/test_client.py -v --tb=short

  full:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install -r requirements.txt -r tests/requirements-test.txt
      - run: pytest -v --tb=short
```

The `unit` job runs on every push with minimal dependencies (~5 s).
The `full` job installs Airflow and runs all 97 tests including DAG integrity.
