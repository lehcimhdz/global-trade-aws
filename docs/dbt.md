# dbt Silver Layer

## Overview

The `dbt/` project transforms raw trade data written by the Airflow pipeline (the **bronze** layer) into clean, typed, and queryable **silver** tables using SQL.

The silver layer runs on top of the **Apache Iceberg** tables registered in the **AWS Glue Data Catalog** (database `comtrade`). Queries are executed by **Amazon Athena** via the `dbt-athena-community` adapter.

```
Bronze (Iceberg / Glue)
        │
        │  [dbt run]
        ▼
   staging views  →  silver Iceberg tables
   (stg_*)            (trade_flows, reporter_summary)
```

---

## Project layout

```
dbt/
├── dbt_project.yml          — project name, model materialization config
├── profiles.yml             — Athena connection (reads AWS env vars)
├── packages.yml             — dbt_utils dependency
├── requirements.txt         — dbt-athena-community pinned version
├── models/
│   ├── sources.yml          — declare bronze Iceberg tables as dbt sources
│   ├── staging/
│   │   ├── stg_preview.sql          — cast + rename /preview columns
│   │   ├── stg_mbs.sql              — cast + rename /getMBS columns
│   │   └── _stg__models.yml         — column docs + schema tests
│   └── silver/
│       ├── trade_flows.sql          — aggregated bilateral trade flows
│       ├── reporter_summary.sql     — per-reporter export/import totals
│       └── _silver__models.yml      — column docs + schema tests
└── tests/
    └── assert_no_negative_trade_value.sql  — custom singular test
```

---

## Layer descriptions

### Sources

Declared in `models/sources.yml`.  Each source maps a dbt name to an Iceberg table in the `comtrade` Glue database:

| dbt source name | Glue table | Description |
|----------------|------------|-------------|
| `bronze.preview` | `comtrade.preview` | Trade flows at commodity level |
| `bronze.previewtariffline` | `comtrade.previewtariffline` | Tariff-line granularity |
| `bronze.getmbs` | `comtrade.getmbs` | Monthly Bulletin of Statistics |

#### Source freshness

The Iceberg writer stamps every record batch with a `_loaded_at` UTC timestamp.  dbt source freshness uses this column to verify that bronze tables are not stale before running expensive model materializations.

```yaml
loaded_at_field: _loaded_at
freshness:
  warn_after:  {count: 26, period: day}
  error_after: {count: 35, period: day}
```

| Threshold | Value | Rationale |
|-----------|-------|-----------|
| `warn_after` | 26 days | Slightly past one monthly cycle — alerts without aborting |
| `error_after` | 35 days | One month + 5-day grace — aborts the dbt run; stale data must not reach silver |

Run manually:

```bash
cd dbt && dbt source freshness
```

### Staging (`+materialized: view`)

Light-weight views that sit directly on top of Iceberg sources.  No data is copied — queries are pushed down to Athena.

| Model | Source | Purpose |
|-------|--------|---------|
| `stg_preview` | `bronze.preview` | Cast types, rename API fields, filter null reporters |
| `stg_mbs` | `bronze.getmbs` | Cast types, rename MBS-specific fields |

Key transformations in `stg_preview`:
- `reportercode → reporter_code` (cast to `integer`)
- `primaryvalue → trade_value_usd` (cast to `double`)
- `period` preserved as `varchar`; `freqcode → freq_code`
- Rows with null `reportercode` or `period` are dropped

### Silver (`+materialized: table`, `+table_type: iceberg`)

Iceberg tables partitioned by `period`, stored in `s3://<bucket>/dbt/silver/`.

| Model | Input | Description |
|-------|-------|-------------|
| `trade_flows` | `stg_preview` | Aggregated bilateral flows — one row per (period, reporter, partner, commodity, flow). Deduplicates partial loads via `SUM`. |
| `reporter_summary` | `stg_preview` | Country-level roll-up — exports, imports, trade balance, commodity count, partner count per reporter per period. |

---

## Schema tests

Every column marked as required has a `not_null` test.  Flow codes are validated with `accepted_values`.  Trade values have `expression_is_true: >= 0` via `dbt_utils`.

| Model | Test | Columns |
|-------|------|---------|
| `stg_preview` | `not_null` | `reporter_code`, `partner_code`, `flow_code`, `period` |
| `stg_preview` | `accepted_values` | `flow_code` → M, X, Re-Import, Re-Export |
| `stg_preview` | `accepted_values` | `freq_code` → A, M |
| `trade_flows` | `not_null` | `reporter_code`, `partner_code`, `flow_code`, `period`, `trade_value_usd` |
| `trade_flows` | `expression_is_true: >= 0` | `trade_value_usd` |
| `reporter_summary` | `expression_is_true: >= 0` | `export_value_usd`, `import_value_usd` |

Custom singular test in `tests/assert_no_negative_trade_value.sql` returns any row with `trade_value_usd < 0` from `stg_preview` — a non-empty result fails the run.

---

## Running dbt

### Prerequisites

```bash
pip install -r dbt/requirements.txt
```

Export the required environment variables (or set them via `.env`):

```bash
export AWS_DEFAULT_REGION=us-east-1
export AWS_ACCESS_KEY_ID=<key>
export AWS_SECRET_ACCESS_KEY=<secret>
export COMTRADE_S3_BUCKET=<your-bucket>
```

### Commands

```bash
cd dbt

# Install dbt packages (dbt_utils)
dbt deps

# Compile SQL without running
dbt compile

# Run all models
dbt run

# Run only staging models
dbt run --select staging

# Run only silver models
dbt run --select silver

# Execute all tests
dbt test

# Run tests for a specific model
dbt test --select trade_flows

# Full CI flow: install → run → test
dbt deps && dbt run && dbt test
```

### Incremental runs

The silver models are full-refresh tables.  To rebuild from scratch:

```bash
dbt run --full-refresh
```

---

## Airflow integration

The `comtrade_dbt` DAG (`dags/comtrade_dbt.py`) orchestrates the full dbt run from Airflow.

### Task order

```
dbt_deps → dbt_source_freshness → dbt_run_staging → dbt_run_silver → dbt_test
```

| Task | Command | Purpose |
|------|---------|---------|
| `dbt_deps` | `dbt deps` | Install / refresh dbt packages (`dbt_utils`) |
| `dbt_source_freshness` | `dbt source freshness` | Verify bronze tables were refreshed within 26/35-day thresholds |
| `dbt_run_staging` | `dbt run --select staging` | Materialise staging views against bronze Iceberg tables |
| `dbt_run_silver` | `dbt run --select silver` | Materialise silver Iceberg tables via Athena |
| `dbt_test` | `dbt test` | Run all schema + custom singular tests |

If `dbt_source_freshness` exits with an error (tables older than 35 days), the downstream tasks are marked skipped in Airflow and the Slack failure callback fires.

### Schedule

`@monthly` — runs at the start of each month, after the ingestion DAGs have had their window to populate the bronze tables.

### Configuration

All AWS credentials and bucket names are injected via Jinja-templated env vars (`{{ var.value.X }}`), so `Variable.get()` is **never** called at parse time.

| Variable | Default | Purpose |
|----------|---------|---------|
| `COMTRADE_DBT_DIR` | `/opt/airflow/dbt` | Path to the dbt project on the worker |
| `COMTRADE_DBT_TARGET` | `prod` | dbt target profile to use |
| `COMTRADE_S3_BUCKET` | — | S3 bucket (forwarded to dbt) |
| `AWS_DEFAULT_REGION` | `us-east-1` | AWS region |
| `AWS_ACCESS_KEY_ID` | `""` | Credentials — omit when using IAM role |
| `AWS_SECRET_ACCESS_KEY` | `""` | Credentials — omit when using IAM role |

### MWAA deployment

In MWAA, include the `dbt/` directory in the artifacts sync step in `.github/workflows/deploy.yml` and set the `COMTRADE_DBT_DIR` Variable to the mounted path.

---

## Configuration reference

| Variable | Where set | Purpose |
|----------|-----------|---------|
| `COMTRADE_S3_BUCKET` | Airflow Variable / env | S3 bucket for staging results and silver data |
| `AWS_DEFAULT_REGION` | Airflow Variable / env | AWS region for Athena and Glue |
| `AWS_ACCESS_KEY_ID` | Airflow Variable / env | Credentials (omit if using IAM role) |
| `AWS_SECRET_ACCESS_KEY` | Airflow Variable / env | Credentials (omit if using IAM role) |
