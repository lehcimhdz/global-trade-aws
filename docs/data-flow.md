# Data Flow

## End-to-end pipeline

```
Airflow Scheduler
      │
      │  (schedule triggers or manual run)
      ▼
 DAG Run created
      │
      ├─── Task 1: extract_and_store_raw
      │          │
      │          │  1. Resolve Airflow Variables (bucket, API params)
      │          │  2. Build API URL from path + query params
      │          │  3. Wait 1.1s (rate-limit guard)
      │          │  4. GET https://comtradeapi.un.org/public/v1/<endpoint>
      │          │  5. Raise on HTTP error (auto-retried up to 3×)
      │          │  6. Serialize response as UTF-8 JSON
      │          │  7. PUT to S3 (Content-Type: application/json)
      │          │  8. Return S3 key → XCom
      │          ▼
      │    S3: comtrade/<endpoint>/type=X/freq=Y/year=YYYY/month=MM/<run_id>.json
      │
      └─── Task 2: convert_to_parquet   (skipped if COMTRADE_WRITE_PARQUET != "true")
                 │
                 │  1. Read S3 key from XCom (output of Task 1)
                 │  2. GET JSON object from S3
                 │  3. Extract response["data"] array
                 │  4. pd.json_normalize(records) → DataFrame
                 │  5. df.to_parquet(engine="pyarrow") → BytesIO
                 │  6. PUT to S3 (Content-Type: application/octet-stream)
                 ▼
           S3: comtrade/<endpoint>/type=X/freq=Y/year=YYYY/month=MM/fmt=parquet/<run_id>.parquet
```

---

## API request lifecycle

```
dag_factory.make_extract_task()
        │
        ▼
  api_kwargs_fn()          ← Variables resolved at runtime (not parse time)
        │
        ▼
  client._get(url, params)
        │
        ├── strip None params  (avoids "key=None" in query string)
        ├── time.sleep(1.1)    (rate-limit: ~1 req/s)
        ├── session.get(url, params, timeout=30)
        │       │
        │       ├── on 429 / 5xx → urllib3 Retry (exponential backoff × 3)
        │       └── on success  → resp.raise_for_status() + resp.json()
        ▼
  raw dict  →  json.dumps()  →  s3.put_object()
```

---

## S3 object structure

### Raw JSON (always written)

```
s3://<COMTRADE_S3_BUCKET>/
  comtrade/
    <endpoint>/
      type=<typeCode>/          ← omitted for endpoints with no typeCode
        freq=<freqCode>/        ← omitted for endpoints with no freqCode
          <extra_partition>/    ← e.g. series_type=T35 for getMBS
            year=<YYYY>/
              month=<MM>/
                <run_id>.json
```

**Example:**
```
comtrade/preview/type=C/freq=A/year=2024/month=03/scheduled__2024-03-01T00-00-00+00-00.json
```

### Parquet (written when COMTRADE_WRITE_PARQUET=true)

Same path with `fmt=parquet/` appended before the filename, and `.parquet` extension:
```
comtrade/preview/type=C/freq=A/year=2024/month=03/fmt=parquet/scheduled__2024-03-01T00-00-00+00-00.parquet
```

### Hive compatibility

All partition directories use `key=value` notation so that AWS Glue crawlers and Amazon Athena can discover the schema without manual table configuration.

---

## Task inter-dependency (XCom)

```
extract_and_store_raw
        │
        │  returns: S3 key string
        │  stored in: XCom (key="return_value")
        ▼
convert_to_parquet(json_key=<XCom value>)
```

The `json_key` argument in `convert_to_parquet` is automatically resolved from XCom by the Airflow TaskFlow API (`@task` decorator).

---

## Retry and error handling

| Layer | Mechanism | Config |
|-------|-----------|--------|
| HTTP transport | `urllib3.Retry` | 3 retries, backoff×2, on 429/5xx |
| Task execution | Airflow `retries` | 2 retries, 5-minute delay |
| Rate limiting | `time.sleep(1.1)` | Applied before every API request |
| Empty data | Log warning, return `None` | Parquet task only |
| HTTP 4xx (non-429) | Raised immediately | No retry (client error) |

---

## Data volume considerations

The Comtrade public API returns at most **500 records per request** (free-tier cap). For full datasets, the subscription API (not covered here) must be used. Each raw JSON file is typically 50–500 KB depending on the endpoint and filter parameters.

---

## DAG schedules and data freshness

| DAG | Endpoint | Schedule | Rationale |
|-----|----------|----------|-----------|
| `comtrade_preview` | `/preview` | Monthly | Trade data published monthly/annually |
| `comtrade_preview_tariffline` | `/previewTariffline` | Monthly | Same publication cadence |
| `comtrade_world_share` | `/getWorldShare` | Monthly | Derived monthly statistics |
| `comtrade_metadata` | `/getMetadata` | Weekly | Classifications rarely change |
| `comtrade_mbs` | `/getMBS` | Monthly | Historical time-series |
| `comtrade_da_tariffline` | `/getDATariffline` | Monthly | Detailed analysis data |
| `comtrade_da` | `/getDA` | Monthly | Detailed analysis data |
| `comtrade_releases` | `/getComtradeReleases` | Daily | Detect new data releases early |

All DAGs have `catchup=False` — only the most recent scheduled interval is run when the DAG is first enabled.
