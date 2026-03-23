# API Reference

All endpoints are from the **UN Comtrade Public API v1**.
Base URL: `https://comtradeapi.un.org/public/v1`

The API is unauthenticated. Rate limit: ~1 request per second. Max 500 records per response (free tier).

---

## Common path parameters

| Parameter | Values | Description |
|-----------|--------|-------------|
| `typeCode` | `C`, `S` | Trade type: Commodities or Services |
| `freqCode` | `A`, `M` | Frequency: Annual or Monthly |
| `clCode` | `HS`, `SITC`, `BEC`, `EB02` | Commodity classification system |

---

## Endpoints

### `preview`

```
GET /preview/{typeCode}/{freqCode}/{clCode}
```

Returns aggregated trade statistics preview (up to 500 records).

**Query parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `reporterCode` | string | Reporter country ISO numeric code(s) |
| `period` | string | Period — annual (`2023`) or monthly (`202301`) |
| `partnerCode` | string | Partner country code(s) |
| `cmdCode` | string | Commodity code(s) |
| `flowCode` | string | Trade flow (`X`, `M`, `re-X`, `re-M`) |

**DAG:** `comtrade_preview` · **Schedule:** Monthly
**S3 path:** `comtrade/preview/type={t}/freq={f}/year=YYYY/month=MM/`

---

### `previewTariffline`

```
GET /previewTariffline/{typeCode}/{freqCode}/{clCode}
```

Returns tariff-line level preview with additional granularity on mode of transport and customs procedures.

**Query parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `reporterCode` | string | Reporter country code(s) |
| `period` | string | Period |
| `partnerCode` | string | Partner country code(s) |
| `partner2Code` | string | Second partner (consignment) code |
| `cmdCode` | string | Commodity code(s) |
| `flowCode` | string | Trade flow |
| `customsCode` | string | Customs procedure code |
| `motCode` | string | Mode of transport code |

**DAG:** `comtrade_preview_tariffline` · **Schedule:** Monthly
**S3 path:** `comtrade/previewTariffline/type={t}/freq={f}/year=YYYY/month=MM/`

---

### `getWorldShare`

```
GET /getWorldShare/{typeCode}/{freqCode}
```

Returns each reporter country's share of global trade for the given period.

**Query parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `period` | string | Period |
| `reporterCode` | string | Reporter country code(s) |

**DAG:** `comtrade_world_share` · **Schedule:** Monthly
**S3 path:** `comtrade/getWorldShare/type={t}/freq={f}/year=YYYY/month=MM/`

---

### `getMetadata`

```
GET /getMetadata/{typeCode}/{freqCode}/{clCode}
```

Returns metadata about the dataset: available reporters, periods, and classification codes for the given type/frequency/classification combination.

No query parameters.

**DAG:** `comtrade_metadata` · **Schedule:** Weekly
**S3 path:** `comtrade/getMetadata/type={t}/freq={f}/year=YYYY/month=MM/`

---

### `getMBS`

```
GET /getMBS
```

Returns historical MBS (Monthly Bulletin of Statistics) time-series data. Covers two tables:
- **T35** — Total trade value (1946–present)
- **T38** — Conversion factors (1946–present)

No path parameters.

**Query parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `series_type` | string | `T35` or `T38` |
| `year` | string | Specific year |
| `country_code` | string | Country code |
| `period` | string | Period |
| `period_type` | string | Period type |
| `table_type` | string | Table sub-type |
| `format` | string | Response format |

**DAG:** `comtrade_mbs` · **Schedule:** Monthly
**S3 path:** `comtrade/getMBS/series_type={s}/year=YYYY/month=MM/`

---

### `getDATariffline`

```
GET /getDATariffline/{typeCode}/{freqCode}/{clCode}
```

Returns detailed analysis data at tariff-line granularity. Subset of data available for direct download.

No query parameters.

**DAG:** `comtrade_da_tariffline` · **Schedule:** Monthly
**S3 path:** `comtrade/getDATariffline/type={t}/freq={f}/year=YYYY/month=MM/`

---

### `getDA`

```
GET /getDA/{typeCode}/{freqCode}/{clCode}
```

Returns detailed analysis data (aggregated, not tariff-line level). Counterpart to `getDATariffline`.

No query parameters.

**DAG:** `comtrade_da` · **Schedule:** Monthly
**S3 path:** `comtrade/getDA/type={t}/freq={f}/year=YYYY/month=MM/`

---

### `getComtradeReleases`

```
GET /getComtradeReleases
```

Returns the schedule of upcoming and recent Comtrade data releases (which countries and periods have been published).

No parameters.

**DAG:** `comtrade_releases` · **Schedule:** Daily
**S3 path:** `comtrade/getComtradeReleases/year=YYYY/month=MM/`

---

## Response format

All endpoints return JSON with this envelope structure:

```json
{
  "elapsedMs": 123,
  "count": 42,
  "data": [
    { ... record ... },
    { ... record ... }
  ]
}
```

The pipeline stores the full envelope as raw JSON. The `convert_to_parquet` task flattens only the `data` array using `pandas.json_normalize()`.

---

## Rate limiting and quotas

| Constraint | Value |
|-----------|-------|
| Requests per second | ~1 |
| Records per response | 500 (free tier) |
| Authentication | Not required (public endpoints) |
| Retry policy (this pipeline) | 3 retries, exponential backoff (2s, 4s, 8s) |
| Inter-request delay | 1.1 seconds (`client.REQUEST_DELAY`) |
