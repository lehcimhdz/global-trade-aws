# Configuration Reference

Configuration lives in two places:

1. **`.env`** — Docker Compose / container-level environment (secrets, ports, image tags)
2. **Airflow Variables** — runtime pipeline parameters read by DAG tasks at execution time

Airflow Variables are the preferred mechanism for anything that can change without a container restart.

---

## Environment file (`.env`)

Copy `.env.example` to `.env` and fill in the required values before starting the stack.

```
cp .env.example .env
```

### Airflow core

| Variable | Default | Description |
|----------|---------|-------------|
| `AIRFLOW_UID` | `50000` | UID used to run Airflow processes inside containers. On Linux set to `$(id -u)`. |
| `AIRFLOW_IMAGE_NAME` | `apache/airflow:2.9.3-python3.11` | Docker image. Change only if pinning a different patch version. |
| `_AIRFLOW_WWW_USER_USERNAME` | `admin` | Webserver login username (created on first init). |
| `_AIRFLOW_WWW_USER_PASSWORD` | `admin` | Webserver login password. **Change in production.** |

### PostgreSQL (Airflow metadata database)

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_USER` | `airflow` | Database user |
| `POSTGRES_PASSWORD` | `airflow` | Database password. **Change in production.** |
| `POSTGRES_DB` | `airflow` | Database name |

### AWS credentials

| Variable | Required | Description |
|----------|----------|-------------|
| `AWS_ACCESS_KEY_ID` | Yes (unless using IAM roles) | AWS access key |
| `AWS_SECRET_ACCESS_KEY` | Yes (unless using IAM roles) | AWS secret key |
| `AWS_DEFAULT_REGION` | No (default: `us-east-1`) | AWS region of the S3 bucket |

> **IAM role alternative:** If the Docker host runs on an EC2 instance or ECS task with an attached IAM role, leave these blank and boto3 will pick up the role automatically via the instance metadata service.

---

## Airflow Variables

Variables are imported from `config/airflow_variables.json` using:

```bash
./scripts/init_variables.sh
```

They can also be set or updated at any time via **Admin → Variables** in the Airflow UI, or with the CLI:

```bash
docker compose exec airflow-webserver airflow variables set COMTRADE_S3_BUCKET my-bucket
```

### Storage

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `COMTRADE_S3_BUCKET` | **Yes** | — | S3 bucket used as the data lake root |

### Trade type / classification

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `COMTRADE_TYPE_CODE` | No | `C` | Trade type. `C` = commodities, `S` = services |
| `COMTRADE_FREQ_CODE` | No | `A` | Frequency. `A` = annual, `M` = monthly |
| `COMTRADE_CL_CODE` | No | `HS` | Commodity classification. Common values: `HS`, `SITC`, `BEC`, `EB02` |

### Filters (optional — omit to retrieve all)

| Variable | Description |
|----------|-------------|
| `COMTRADE_REPORTER_CODE` | Reporter country ISO numeric code(s). Multiple values comma-separated, e.g. `840,276` |
| `COMTRADE_PERIOD` | Period filter. Annual: `2023`. Monthly: `202301`. Multiple: `2022,2023` |
| `COMTRADE_PARTNER_CODE` | Partner country code(s) |
| `COMTRADE_CMD_CODE` | Commodity code(s) |
| `COMTRADE_FLOW_CODE` | Trade flow. `X` = export, `M` = import, `re-X` = re-export, `re-M` = re-import |

### Tariff-line specific

| Variable | Description |
|----------|-------------|
| `COMTRADE_PARTNER2_CODE` | Second partner code (used in `previewTariffline`) |
| `COMTRADE_CUSTOMS_CODE` | Customs procedure code |
| `COMTRADE_MOT_CODE` | Mode of transport code |

### MBS specific

| Variable | Default | Description |
|----------|---------|-------------|
| `COMTRADE_MBS_SERIES_TYPE` | `T35` | MBS table. `T35` = total trade value (1946–), `T38` = conversion factors |
| `COMTRADE_MBS_YEAR` | — | Specific year filter |
| `COMTRADE_MBS_COUNTRY_CODE` | — | Country filter |
| `COMTRADE_MBS_PERIOD` | — | Period filter |
| `COMTRADE_MBS_PERIOD_TYPE` | — | Period type |
| `COMTRADE_MBS_TABLE_TYPE` | — | Table type |
| `COMTRADE_MBS_FORMAT` | — | Response format |

### Processing

| Variable | Default | Description |
|----------|---------|-------------|
| `COMTRADE_WRITE_PARQUET` | `false` | Set to `"true"` to enable the `convert_to_parquet` task in every DAG. Requires `pandas` and `pyarrow`. |

### dbt (silver layer)

| Variable | Default | Description |
|----------|---------|-------------|
| `COMTRADE_DBT_DIR` | `/opt/airflow/dbt` | Absolute path to the dbt project inside the Airflow container. Change if mounting the dbt project at a different path. |
| `COMTRADE_DBT_TARGET` | `prod` | dbt target profile to use. `prod` uses the Athena `comtrade` workgroup; `dev` uses the `primary` workgroup. |

### AWS (can also be set via `.env`)

Variables `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_DEFAULT_REGION` are read by `s3_writer.py` if present as Airflow Variables. The `.env` values are also available as container environment variables, so setting them in `.env` is sufficient — the Variables in `airflow_variables.json` can be left empty.

---

## Precedence

```
Airflow Variable  >  Container environment variable  >  boto3 default chain (IAM role / ~/.aws)
```

---

## Configuration for common scenarios

### Annual HS commodity exports from the US to Germany

```json
{
  "COMTRADE_TYPE_CODE": "C",
  "COMTRADE_FREQ_CODE": "A",
  "COMTRADE_CL_CODE": "HS",
  "COMTRADE_REPORTER_CODE": "842",
  "COMTRADE_PARTNER_CODE": "276",
  "COMTRADE_FLOW_CODE": "X",
  "COMTRADE_PERIOD": "2023"
}
```

### Monthly total world trade

```json
{
  "COMTRADE_TYPE_CODE": "C",
  "COMTRADE_FREQ_CODE": "M",
  "COMTRADE_CL_CODE": "HS",
  "COMTRADE_REPORTER_CODE": "",
  "COMTRADE_PERIOD": "202312"
}
```

### MBS historical series for all countries

```json
{
  "COMTRADE_MBS_SERIES_TYPE": "T35",
  "COMTRADE_MBS_YEAR": "",
  "COMTRADE_MBS_COUNTRY_CODE": ""
}
```
