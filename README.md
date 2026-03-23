# global-trade-aws

Apache Airflow pipeline that extracts trade data from the [UN Comtrade public API](https://comtradeapi.un.org) and stores it in AWS S3 as a data lake.

## Documentation

| Document | Description |
|----------|-------------|
| [docs/architecture.md](docs/architecture.md) | System architecture, component responsibilities, infrastructure topology |
| [docs/data-flow.md](docs/data-flow.md) | End-to-end pipeline flow, S3 structure, retry behaviour |
| [docs/api-reference.md](docs/api-reference.md) | All 8 Comtrade endpoints, parameters, and rate limits |
| [docs/configuration.md](docs/configuration.md) | All Airflow Variables and `.env` settings with examples |
| [docs/plugins.md](docs/plugins.md) | Plugin system internals — client, S3 writer, DAG factory |
| [docs/operations.md](docs/operations.md) | Deployment, monitoring, troubleshooting, IAM policy |
| [docs/testing.md](docs/testing.md) | Test suite structure, how to run, mocking strategy, CI integration |

---

## Project structure

```
.
├── dags/                          # One DAG file per Comtrade endpoint
│   ├── comtrade_preview.py
│   ├── comtrade_preview_tariffline.py
│   ├── comtrade_world_share.py
│   ├── comtrade_metadata.py
│   ├── comtrade_mbs.py
│   ├── comtrade_da_tariffline.py
│   ├── comtrade_da.py
│   └── comtrade_releases.py
├── plugins/
│   └── comtrade/
│       ├── __init__.py
│       ├── client.py              # API calls + retry logic
│       ├── s3_writer.py           # S3 upload helpers
│       └── dag_factory.py         # Shared task factories (DRY)
├── config/
│   └── airflow_variables.json     # Seed file for Airflow Variables
├── scripts/
│   └── init_variables.sh          # Import Variables into running stack
├── logs/                          # Airflow task logs (gitignored)
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

## DAGs

| DAG | Endpoint | Schedule |
|-----|----------|----------|
| `comtrade_preview` | `/preview/{typeCode}/{freqCode}/{clCode}` | Monthly |
| `comtrade_preview_tariffline` | `/previewTariffline/{typeCode}/{freqCode}/{clCode}` | Monthly |
| `comtrade_world_share` | `/getWorldShare/{typeCode}/{freqCode}` | Monthly |
| `comtrade_metadata` | `/getMetadata/{typeCode}/{freqCode}/{clCode}` | Weekly |
| `comtrade_mbs` | `/getMBS` | Monthly |
| `comtrade_da_tariffline` | `/getDATariffline/{typeCode}/{freqCode}/{clCode}` | Monthly |
| `comtrade_da` | `/getDA/{typeCode}/{freqCode}/{clCode}` | Monthly |
| `comtrade_releases` | `/getComtradeReleases` | Daily |

Each DAG has two tasks:
1. **`extract_and_store_raw`** — calls the API and writes raw JSON to S3
2. **`convert_to_parquet`** — converts the response to Parquet (only when `COMTRADE_WRITE_PARQUET=true`)

## S3 key layout

```
s3://<bucket>/comtrade/<endpoint>/type=<typeCode>/freq=<freqCode>/year=YYYY/month=MM/<run_id>.json
s3://<bucket>/comtrade/<endpoint>/type=<typeCode>/freq=<freqCode>/year=YYYY/month=MM/fmt=parquet/<run_id>.parquet
```

Hive-compatible partitions so Athena / Glue can crawl the bucket without extra configuration.

## Quick start

### 1. Copy and configure the environment file

```bash
cp .env.example .env
# Edit .env — set AWS credentials, S3 bucket, and any Comtrade filter params
```

### 2. Set the Airflow UID (Linux only)

```bash
echo -e "AIRFLOW_UID=$(id -u)" >> .env
```

### 3. Start the stack

```bash
docker compose up airflow-init   # run once to initialise the DB
docker compose up -d             # start all services
```

The Airflow UI is at **http://localhost:8080** (default credentials: `admin` / `admin`).

### 4. Import Airflow Variables

```bash
./scripts/init_variables.sh
```

Or go to **Admin → Variables** in the UI and update the values, especially:
- `COMTRADE_S3_BUCKET`
- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`

### 5. Trigger a DAG

```bash
docker compose exec airflow-webserver airflow dags trigger comtrade_preview
```

Or unpause and trigger from the UI.

## Airflow Variables reference

| Variable | Default | Description |
|----------|---------|-------------|
| `COMTRADE_S3_BUCKET` | _(required)_ | Target S3 bucket |
| `COMTRADE_TYPE_CODE` | `C` | Trade type: `C`=commodities, `S`=services |
| `COMTRADE_FREQ_CODE` | `A` | Frequency: `A`=annual, `M`=monthly |
| `COMTRADE_CL_CODE` | `HS` | Classification: `HS`, `SITC`, `BEC`, … |
| `COMTRADE_REPORTER_CODE` | _(all)_ | Reporter country ISO numeric code(s) |
| `COMTRADE_PERIOD` | _(latest)_ | Period filter, e.g. `2023` or `202301` |
| `COMTRADE_PARTNER_CODE` | _(all)_ | Partner country code(s) |
| `COMTRADE_CMD_CODE` | _(all)_ | Commodity code(s) |
| `COMTRADE_FLOW_CODE` | _(all)_ | Flow: `X`=export, `M`=import, `re-X`, `re-M` |
| `COMTRADE_WRITE_PARQUET` | `false` | Set `true` to also write Parquet |
| `AWS_ACCESS_KEY_ID` | _(env)_ | AWS key (can also come from env/IAM role) |
| `AWS_SECRET_ACCESS_KEY` | _(env)_ | AWS secret |
| `AWS_DEFAULT_REGION` | `us-east-1` | AWS region |

## Dependencies

See `requirements.txt`. Key packages:
- `apache-airflow[amazon]` 2.9.3
- `apache-airflow-providers-amazon`
- `boto3`
- `requests`
- `pandas` + `pyarrow` (only needed for Parquet conversion)
