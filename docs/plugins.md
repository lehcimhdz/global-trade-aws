# Plugin System

The `plugins/comtrade/` package contains all shared logic. It is mounted into every Airflow container at `/opt/airflow/plugins/` and is automatically added to `sys.path` by Airflow, making it importable from DAG files as `from comtrade import ...`.

---

## Module overview

```
plugins/
└── comtrade/
    ├── __init__.py       — package marker
    ├── client.py         — HTTP client for the Comtrade API
    ├── s3_writer.py      — S3 upload utilities
    ├── dag_factory.py    — @task factory functions shared by all DAGs
    ├── validator.py      — Pure-Python data quality checks (no Airflow dependency)
    ├── callbacks.py      — Slack failure notifications, SLA miss alerts, dead-letter S3 manifest
    ├── metrics.py        — CloudWatch custom metric emission (RowCount, ChecksPassed, …)
    ├── lineage.py        — OpenLineage event emission to Marquez
    ├── schema.py         — Schema drift detection and Slack alerting
    └── iceberg.py        — Apache Iceberg writer (ACID appends to Glue-backed tables)
```

---

## `client.py`

### Purpose

Wraps all 8 Comtrade API endpoints behind typed Python functions. Handles:

- Rate limiting (1.1-second sleep before every request)
- Retry logic via `urllib3.Retry` (3 attempts, exponential backoff, on 429/5xx)
- Query parameter sanitization (removes `None` values)
- JSON deserialization

### Session setup

A single `requests.Session` is created at module import time and reused across all calls in the same worker process. The retry adapter is attached once:

```python
retry = Retry(
    total=3,
    backoff_factor=2,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)
```

### Functions

| Function | Endpoint |
|----------|----------|
| `get_preview(typeCode, freqCode, clCode, ...)` | `/preview/{t}/{f}/{c}` |
| `get_preview_tariffline(typeCode, freqCode, clCode, ...)` | `/previewTariffline/{t}/{f}/{c}` |
| `get_world_share(typeCode, freqCode, ...)` | `/getWorldShare/{t}/{f}` |
| `get_metadata(typeCode, freqCode, clCode)` | `/getMetadata/{t}/{f}/{c}` |
| `get_mbs(series_type, year, ...)` | `/getMBS` |
| `get_da_tariffline(typeCode, freqCode, clCode)` | `/getDATariffline/{t}/{f}/{c}` |
| `get_da(typeCode, freqCode, clCode)` | `/getDA/{t}/{f}/{c}` |
| `get_comtrade_releases()` | `/getComtradeReleases` |

All functions accept only the parameters that endpoint supports. Optional parameters default to `None` and are stripped before the HTTP call.

### Adding a new endpoint

1. Add a function following the same pattern as the existing ones.
2. Use `_get(url, params)` — do not call `requests.get` directly.
3. Keep `None`-defaulted optional kwargs; `_get` strips them automatically.

---

## `s3_writer.py`

### Purpose

Provides two upload functions and a key-builder that enforces the Hive-partitioned path convention.

### `build_s3_key()`

```python
def build_s3_key(
    endpoint: str,
    run_id: str,
    year: str,
    month: str,
    typeCode: Optional[str] = None,
    freqCode: Optional[str] = None,
    extra_partitions: Optional[Dict[str, str]] = None,
    fmt: str = "json",
) -> str
```

Constructs the S3 object key. Partition segments (`type=`, `freq=`, and any `extra_partitions`) are omitted when not provided, so endpoints without path params (e.g., `getComtradeReleases`) produce shorter keys.

**Output examples:**

```
# With typeCode and freqCode
comtrade/preview/type=C/freq=A/year=2024/month=03/run_id.json

# With extra_partitions only (MBS)
comtrade/getMBS/series_type=T35/year=2024/month=03/run_id.json

# No partitions (releases)
comtrade/getComtradeReleases/year=2024/month=03/run_id.json

# Parquet variant
comtrade/preview/type=C/freq=A/year=2024/month=03/fmt=parquet/run_id.parquet
```

### `write_json_to_s3(data, bucket, key)`

Serializes `data` to UTF-8 JSON and uploads to S3 with `Content-Type: application/json`. Uses `default=str` in `json.dumps` to safely handle non-serializable types (dates, Decimals).

### `write_parquet_to_s3(records, bucket, key)`

Accepts a list of dicts, normalizes them with `pandas.json_normalize()`, serializes to Parquet (PyArrow engine), and uploads. `pandas` is imported inside the function body — it is not loaded unless this function is actually called, avoiding overhead on workers where Parquet is disabled.

### Credentials

`s3_writer.py` creates a boto3 client at call time using:

```
AWS_ACCESS_KEY_ID      → Airflow Variable → falls back to boto3 default chain
AWS_SECRET_ACCESS_KEY  → Airflow Variable → falls back to boto3 default chain
AWS_DEFAULT_REGION     → Airflow Variable (default: us-east-1)
```

---

## `dag_factory.py`

### Purpose

Generates the three standard `@task` functions that every DAG in this project needs. Using a factory avoids copy-pasting identical task bodies across 8 DAG files.

### `make_extract_task()`

```python
def make_extract_task(
    endpoint: str,
    api_fn: Callable,
    api_kwargs_fn: Callable[[], Dict],
    typeCode: Optional[str] = None,
    freqCode: Optional[str] = None,
    extra_partitions: Optional[Dict[str, str]] = None,
) -> Callable
```

Returns an `@task(task_id="extract_and_store_raw")` function. When Airflow executes it:

1. Resolves `logical_date` and `run_id` from the task context.
2. Calls `api_kwargs_fn()` — a zero-argument lambda defined in the DAG file that reads Airflow Variables at runtime (not parse time, which is important for lazy evaluation).
3. Calls `api_fn(**kwargs)` to fetch data.
4. Builds the S3 key via `build_s3_key()`.
5. Uploads the raw response JSON.
6. Returns the S3 key string (stored in XCom for downstream tasks).

**Why `api_kwargs_fn` instead of passing kwargs directly?**
Airflow evaluates DAG-level code at parse time (every ~30 seconds). `Variable.get()` called at parse time would hit the database on every parse cycle. Wrapping Variable reads in a lambda defers them to task execution time, reducing database load and avoiding errors when Variables don't exist yet.

### `make_validate_task()`

```python
def make_validate_task(
    endpoint: str,
    required_columns: Optional[List[str]] = None,
    numeric_columns: Optional[List[str]] = None,
    dedup_columns: Optional[List[str]] = None,
    min_rows: int = 1,
    freq_code_variable: Optional[str] = "COMTRADE_FREQ_CODE",
) -> Callable
```

Returns an `@task(task_id="validate_bronze")` function. When executed:

1. Downloads the bronze JSON from S3 using the key passed via XCom.
2. Calls `validator.run_checks()` with the per-DAG check configuration.
3. Calls `validator.assert_quality()` — logs all results; raises `DataQualityError` on any ERROR-severity failure.
4. Returns the same S3 key as a pass-through for the parquet task.

`freq_code_variable` controls whether period-format validation runs. Set to `None` for endpoints that don't have a `period` field (metadata, releases, MBS).

### `make_parquet_task()`

```python
def make_parquet_task(
    endpoint: str,
    typeCode: Optional[str] = None,
    freqCode: Optional[str] = None,
    extra_partitions: Optional[Dict[str, str]] = None,
) -> Callable
```

Returns an `@task(task_id="convert_to_parquet")` function. When executed:

1. Checks `COMTRADE_WRITE_PARQUET` Variable — skips with a log message if not `"true"`.
2. Downloads the JSON file from S3 using the key passed via XCom.
3. Extracts the `data` array from the response envelope.
4. Calls `write_parquet_to_s3()` and returns the Parquet S3 key (or `None` if skipped/empty).

---

## How DAG files use the factory

```python
# dags/comtrade_preview.py (simplified)

from comtrade import client
from comtrade.callbacks import sla_miss_callback
from comtrade.dag_factory import make_extract_task, make_parquet_task, make_validate_task

with DAG(
    "comtrade_preview",
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5), "sla": timedelta(hours=8)},
    sla_miss_callback=sla_miss_callback,
    ...
) as dag:

    def _api_kwargs():          # lambda — evaluated at task runtime
        return dict(
            typeCode=Variable.get("COMTRADE_TYPE_CODE", default_var="C"),
            ...
        )

    extract = make_extract_task(
        endpoint="preview",
        api_fn=client.get_preview,
        api_kwargs_fn=_api_kwargs,
        typeCode=Variable.get("COMTRADE_TYPE_CODE", default_var="C"),
        freqCode=Variable.get("COMTRADE_FREQ_CODE", default_var="A"),
    )()

    validated = make_validate_task(
        endpoint="preview",
        required_columns=["reporterCode", "period"],
        numeric_columns=["primaryValue"],
        dedup_columns=["reporterCode", "partnerCode", "cmdCode", "flowCode", "period"],
    )(json_key=extract)          # ← XCom dependency wired automatically

    to_parquet = make_parquet_task(
        endpoint="preview",
        typeCode=...,
        freqCode=...,
    )(json_key=validated)        # ← receives key from validate, not extract
```

The TaskFlow API (`@task`) resolves XCom dependencies implicitly from the function arguments.

---

## `validator.py`

### Purpose

Pure-Python data quality checks with no Airflow dependency. Because it imports nothing from Airflow it can be unit-tested without an Airflow installation.

### Check functions

| Function | Severity | What it checks |
|----------|----------|----------------|
| `check_envelope` | ERROR | Response is a `dict` or `list` |
| `check_has_data_key` | ERROR | Dict has a `data` key containing a list |
| `check_row_count` | ERROR | At least `min_rows` records present |
| `check_no_nulls` | ERROR | Required columns have no null/empty values |
| `check_numeric_non_negative` | WARNING | Numeric columns are ≥ 0 |
| `check_period_format` | ERROR | Period values match `YYYY` (annual) or `YYYYMM` (monthly) |
| `check_no_duplicates` | WARNING | No duplicate natural key combinations |

### `run_checks(data, ...)` → `List[CheckResult]`

Runs the full suite and returns all results regardless of pass/fail. The caller decides what to do with them.

### `assert_quality(results)`

Logs every result and raises `DataQualityError` if any ERROR-severity check failed. WARNING failures are logged but do not raise.

### `CheckResult` and `Severity`

```python
class Severity(str, Enum):
    ERROR = "error"    # fails the DAG run
    WARNING = "warning"  # logged only

@dataclass
class CheckResult:
    name: str
    passed: bool
    message: str
    severity: Severity = Severity.ERROR
    details: Dict[str, Any] = field(default_factory=dict)
```

---

## `callbacks.py`

### Purpose

Three responsibilities, all using Python stdlib only (`urllib.request`, `json`) — no extra dependencies. All Airflow imports are lazy.

### `task_failure_callback(context)`

Airflow `on_failure_callback`. Attached to every task via `dag_factory.py`. Does two things in order:

1. **Dead-letter manifest** — writes a structured JSON file to `s3://<bucket>/comtrade/errors/dag_id=.../task_id=.../year=.../month=.../` for every failure, regardless of Slack configuration. Queryable with Athena.
2. **Slack alert** — posts a Block Kit message with DAG ID, task ID, run ID, execution date, exception snippet, and a log link.

### `sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis)`

Airflow `sla_miss_callback`. Set on each DAG. Posts a Slack message listing missed tasks, blocking tasks, and execution dates.

### Resilience

- If `COMTRADE_SLACK_WEBHOOK_URL` is not set, a warning is logged and the function returns silently.
- The dead-letter write always runs first, independently of Slack.
- Any exception is caught and logged — notification failures never mask the original task failure.

### Configuration

```dotenv
COMTRADE_SLACK_WEBHOOK_URL=https://hooks.slack.com/services/T.../B.../...
```

Create a webhook at <https://api.slack.com/messaging/webhooks>.

---

## `metrics.py`

### Purpose

Emits four CloudWatch custom metrics under the `Comtrade/Pipeline` namespace after every `validate_bronze` run. All boto3 imports are lazy; errors are caught and logged without propagating.

### Metrics

| Metric | Unit | Description |
|--------|------|-------------|
| `RowCount` | Count | Records returned by the API |
| `ChecksPassed` | Count | Quality checks that passed |
| `ChecksFailed` | Count | Quality checks that failed (any severity) |
| `JsonBytesWritten` | Bytes | Size of the S3 bronze JSON object |

All metrics carry two dimensions: **DagId** and **Endpoint**.

### `emit_validation_metrics(dag_id, endpoint, results, json_bytes)`

Called automatically from `make_validate_task()`. Builds the metric payload from `CheckResult` objects and calls `cloudwatch.put_metric_data`.

---

## `lineage.py`

### Purpose

Emits [OpenLineage](https://openlineage.io) `RunEvent` JSON to a Marquez-compatible HTTP endpoint. Uses Python stdlib (`urllib.request`) — no `openlineage-python` package required. All Airflow imports are lazy.

### Lineage graph

```
comtradeapi.un.org/public/v1/<endpoint>
    │
    │  [extract_and_store_raw]
    ▼
s3://<bucket>/comtrade/<endpoint>/…/<run_id>.json   (bronze)
    │
    ├──[validate_bronze]──►  (quality gate, no new dataset)
    │
    └──[convert_to_parquet]
         │
         ▼
       s3://<bucket>/comtrade/<endpoint>/…/fmt=parquet/<run_id>.parquet   (silver)
```

### Dataset naming convention

| URI type | Namespace | Name |
|----------|-----------|------|
| `s3://bucket/key` | `s3://bucket` | `key` |
| `https://host/path` | `https://host` | `/path` |

### `emit_task_complete(dag_id, task_id, run_id, input_uris, output_uris)`

Called automatically from `dag_factory.py` after each task succeeds. Posts a `COMPLETE` event to `<OPENLINEAGE_URL>/api/v1/lineage`. If `OPENLINEAGE_URL` is not set the call is silently skipped.

### Configuration

Add to `.env`:

```dotenv
OPENLINEAGE_URL=http://marquez:5000
```

Or start the bundled Marquez stack (see [Operations — Data lineage](operations.md#data-lineage-marquez)).

---

## `schema.py`

### Purpose

Detects column-set changes (schema drift) in the Comtrade API responses between pipeline runs. The column set from each run is compared against a per-endpoint baseline stored in S3; if it differs, a Slack alert is sent and the baseline is updated. All Airflow / boto3 imports are lazy; errors are caught without propagating.

### Schema storage

Baselines are stored as small JSON files in the data lake:

```
s3://<COMTRADE_S3_BUCKET>/comtrade/schemas/<endpoint>.json
```

Format:

```json
{
  "schema_version": "1",
  "endpoint": "preview",
  "columns": ["cmdCode", "flowCode", "period", "primaryValue", "..."],
  "column_count": 42,
  "captured_at": "2024-03-01T00:00:00+00:00",
  "run_id": "scheduled__2024-03-01T00-00-00"
}
```

### Drift lifecycle

| Run | Previous schema | Current columns | Action |
|-----|----------------|-----------------|--------|
| 1st | Not found | Any | Save baseline, INFO logged |
| Nth | Identical | Same | No-op, DEBUG logged |
| Nth | Differs | Added and/or removed | WARNING logged + Slack alert + baseline updated |

The baseline is **always updated to the new column set** after drift so the next run compares against the current API shape.

### `SchemaDriftResult`

```python
@dataclass
class SchemaDriftResult:
    endpoint: str
    previous_columns: Set[str]
    current_columns: Set[str]
    added: Set[str]    # current - previous
    removed: Set[str]  # previous - current
    has_drift: bool    # property: bool(added or removed)
```

### `detect_and_alert(bucket, endpoint, records, run_id)`

The main entry point, called automatically from `make_validate_task()` before quality checks. Extracts the union of all keys across `records` (defensive — handles sparse API responses where some columns appear only in certain rows), compares against the stored baseline, and alerts on change.

Returns a `SchemaDriftResult` if drift was found, `None` otherwise.

### Configuration

No extra configuration required. Reuses `COMTRADE_SLACK_WEBHOOK_URL` for alerts and the existing S3 credentials. If the webhook is not set, the alert is suppressed and a warning is logged.

---

## `iceberg.py`

### Purpose

Appends validated trade records to a **Glue-backed Apache Iceberg table**, providing ACID writes, time travel, partition evolution, and automatic schema evolution. All `pyiceberg` imports are lazy — the module can be imported and tested without the package installed. Errors are caught and logged; Iceberg failures never fail the pipeline.

### Table layout

| Property | Value |
|----------|-------|
| Glue database | `comtrade` |
| Glue table | `<endpoint>` (e.g., `preview`, `getMBS`) |
| S3 location | `s3://<bucket>/iceberg/<endpoint>/` |

### Functions

#### `write_to_iceberg(records, endpoint, bucket, region)`

The main entry point. Converts *records* to a PyArrow Table, opens or creates the Glue-backed Iceberg table, and appends the data in a single ACID commit.

- If the table already exists and the current records contain new columns, the schema is evolved automatically via `union_by_name` — no manual migration required.
- Returns the Iceberg table identifier (`comtrade.<endpoint>`) on success, `None` on failure or when *records* is empty.

#### Internal helpers (not part of the public API)

| Function | Purpose |
|----------|---------|
| `_warehouse_uri(bucket)` | `s3://<bucket>/iceberg` |
| `_table_identifier(endpoint)` | `comtrade.<endpoint>` |
| `_table_location(bucket, endpoint)` | `s3://<bucket>/iceberg/<endpoint>` |
| `_records_to_pa_table(records)` | Convert list-of-dicts to PyArrow Table |
| `_get_catalog(region, warehouse)` | Load PyIceberg Glue catalog |
| `_ensure_namespace(catalog, database)` | Create Glue database if missing |
| `_load_or_create_table(catalog, identifier, pa_table, location)` | Load or create Iceberg table, evolve schema |

### `make_iceberg_task()` (in `dag_factory.py`)

```python
def make_iceberg_task(endpoint: str) -> Callable
```

Returns an `@task(task_id="write_to_iceberg")` function. When Airflow executes it:

1. Checks `COMTRADE_WRITE_ICEBERG` Variable — skips if not `"true"`.
2. Downloads the validated bronze JSON from S3 using the key passed via XCom.
3. Extracts the `data` array from the response envelope.
4. Calls `write_to_iceberg()` and returns the Iceberg table identifier (or `None` if skipped/empty).
5. Emits an OpenLineage `COMPLETE` event on success.

### Configuration

```dotenv
COMTRADE_WRITE_ICEBERG=true
```

Requires `pyiceberg[glue,pyarrow]` in `requirements.txt` and the Glue Data Catalog database to exist (created by `terraform/glue.tf`). IAM permissions for `glue:CreateTable`, `glue:GetTable`, etc. are added to the Airflow IAM policy in `terraform/iam.tf`.
