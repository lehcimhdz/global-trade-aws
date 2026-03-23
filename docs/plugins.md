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
    └── dag_factory.py    — @task factory functions shared by all DAGs
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

Generates the two standard `@task` functions that every DAG in this project needs. Using a factory avoids copy-pasting identical task bodies across 8 DAG files.

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
from comtrade.dag_factory import make_extract_task, make_parquet_task

with DAG("comtrade_preview", ...) as dag:

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
    )()                          # ← call returns the decorated task

    to_parquet = make_parquet_task(
        endpoint="preview",
        typeCode=...,
        freqCode=...,
    )(json_key=extract)          # ← XCom dependency wired automatically
```

The `(json_key=extract)` call passes the output of `extract` as an XCom input — the TaskFlow API (`@task`) handles the dependency implicitly.
