# Operations Guide

## Initial setup

### Prerequisites

- Docker Engine 24+ and Docker Compose v2
- AWS account with an S3 bucket and an IAM user/role with `s3:PutObject` and `s3:GetObject` permissions on that bucket
- Ports `8080` (Airflow UI) and `5432`, `6379` available on the host (internal only for the last two)

### Step-by-step deployment

**1. Clone the repository and enter the directory**

```bash
git clone <repo-url>
cd global-trade-aws
```

**2. Create the environment file**

```bash
cp .env.example .env
```

Edit `.env` and set at minimum:

```dotenv
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=us-east-1
COMTRADE_S3_BUCKET=my-data-lake-bucket
_AIRFLOW_WWW_USER_PASSWORD=changeme
```

On Linux, also add:

```bash
echo "AIRFLOW_UID=$(id -u)" >> .env
```

**3. Create the `logs/` directory with the correct ownership**

```bash
mkdir -p logs
```

**4. Initialize the database (run once)**

```bash
docker compose up airflow-init
```

Wait until the container exits with code 0 before proceeding.

**5. Start all services**

```bash
docker compose up -d
```

Check that all containers are healthy:

```bash
docker compose ps
```

All services should show `healthy` within ~60 seconds.

**6. Import Airflow Variables**

```bash
./scripts/init_variables.sh
```

Then open `http://localhost:8080` → **Admin → Variables** and set `COMTRADE_S3_BUCKET` to your actual bucket name (and AWS credentials if not using the environment approach).

**7. Enable and trigger the first DAG**

In the UI, unpause `comtrade_preview` (toggle in the DAG list) and click **Trigger DAG** to run it immediately.

---

## Day-to-day operations

### Viewing DAG runs and logs

- Open `http://localhost:8080`
- Select a DAG → **Grid view** for run history
- Click any task square → **Log** tab for full stdout

### Triggering a DAG manually

```bash
docker compose exec airflow-webserver \
  airflow dags trigger comtrade_preview
```

With a configuration override:

```bash
docker compose exec airflow-webserver \
  airflow dags trigger comtrade_preview \
  --conf '{"COMTRADE_PERIOD": "2024"}'
```

> Note: DAG conf overrides are available in `context["dag_run"].conf` but the current tasks read from Airflow Variables. To pass one-off parameters, update the Variable before triggering.

### Updating a Variable

```bash
docker compose exec airflow-webserver \
  airflow variables set COMTRADE_PERIOD 2024
```

Or in the UI: **Admin → Variables → Edit**.

### Scaling workers

Add more Celery workers by scaling the service:

```bash
docker compose up -d --scale airflow-worker=3
```

### Stopping the stack

```bash
docker compose down
```

To also remove the database volume (destructive — deletes all run history):

```bash
docker compose down -v
```

---

## Alerting

### Slack failure notifications

Every task in every DAG sends a Slack message when it fails (after all retries are exhausted). The message includes the DAG ID, task ID, run ID, execution date, the exception message, and a direct link to the task log.

**Setup:**

1. Create a Slack Incoming Webhook at <https://api.slack.com/messaging/webhooks>.
2. Add the URL to `.env`:
   ```dotenv
   COMTRADE_SLACK_WEBHOOK_URL=https://hooks.slack.com/services/T.../B.../...
   ```
3. Push to Secrets Manager:
   ```bash
   make bootstrap-secrets ENV=dev
   ```

If the variable is not set the callbacks log a warning and return silently — local development works without a Slack workspace.

### SLA miss notifications

Each DAG has an SLA window based on its schedule. If the pipeline hasn't completed within that window after the scheduled execution date, Airflow fires a Slack SLA miss alert listing which tasks missed and which are blocking.

| Schedule | SLA window |
|----------|-----------|
| `@monthly` | 8 hours |
| `@weekly` | 4 hours |
| `@daily` | 2 hours |

The same `COMTRADE_SLACK_WEBHOOK_URL` variable is used for SLA alerts.

---

## Monitoring

### Airflow UI dashboards

| View | URL | Use |
|------|-----|-----|
| DAG list | `/` | Overview of all DAGs, last run status |
| Grid view | `/dags/<dag_id>/grid` | Run history, task success/failure matrix |
| Task log | Click any task cell → Log | Full stdout/stderr |
| Variables | `/variable/list/` | Inspect and edit runtime config |

### CloudWatch dashboard

After Terraform is applied, the `cloudwatch_dashboard_url` output gives a direct link to the ops-facing dashboard. It shows:

| Widget | What it tracks |
|--------|----------------|
| Row Count per DAG Run | Data volume over time |
| Quality Gate — Checks Passed vs Failed | DAG health |
| Check Failure Rate (%) | API error rate |
| JSON Bytes Written to S3 | Raw data volume |

Metrics are emitted automatically by `validate_bronze` via `metrics.py`. The IAM policy already grants `cloudwatch:PutMetricData` scoped to the `Comtrade/Pipeline` namespace.

### Data lineage (Marquez)

Start the optional Marquez stack alongside Airflow:

```bash
docker compose --profile lineage up -d
```

| UI | URL |
|----|-----|
| Marquez API | `http://localhost:5000` |
| Marquez Web UI | `http://localhost:3000` |

Then set the Airflow Variable so workers know where to send events:

```bash
docker compose exec airflow-webserver \
  airflow variables set OPENLINEAGE_URL http://marquez:5000
```

Once events arrive you can browse the lineage graph at `http://localhost:3000` — select the `comtrade` namespace to see dataset dependencies across all DAGs.

If `OPENLINEAGE_URL` is not set, lineage emission is silently skipped — Airflow and the rest of the pipeline work normally.

### Celery Flower (optional)

Enable the Flower service to monitor worker queues and task throughput:

```bash
docker compose --profile flower up -d flower
```

Flower UI is available at `http://localhost:5555`.

### S3 data verification

List the most recent objects for an endpoint:

```bash
aws s3 ls s3://<bucket>/comtrade/preview/ --recursive | sort | tail -20
```

Read the latest JSON file:

```bash
aws s3 cp s3://<bucket>/comtrade/preview/type=C/freq=A/year=2024/month=03/<run_id>.json - | python3 -m json.tool | head -50
```

---

## Troubleshooting

### DAG not appearing in the UI

- Check scheduler logs: `docker compose logs airflow-scheduler`
- Look for Python import errors — most commonly a missing dependency or a typo in a DAG file.
- All DAGs start **paused**. Unpause them in the UI or with:
  ```bash
  docker compose exec airflow-webserver airflow dags unpause comtrade_preview
  ```

### Task fails with `NoCredentialsError`

AWS credentials are not reachable. Options:
1. Ensure `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` are set in `.env`.
2. Or set them as Airflow Variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`).
3. On EC2/ECS, verify the instance profile/task role has `s3:PutObject` on the target bucket.

### Task fails with `HTTP 429`

The API rate limit was hit. The built-in retry with exponential backoff will handle transient 429s. If it persists:
- Check that only one Airflow worker is running (parallel DAG runs could multiply requests).
- Increase `REQUEST_DELAY` in `plugins/comtrade/client.py`.

### Task fails with `KeyError: 'COMTRADE_S3_BUCKET'`

The Variable has not been imported. Run:

```bash
./scripts/init_variables.sh
```

Then set the value in the UI.

### Parquet task produces no output

- Confirm `COMTRADE_WRITE_PARQUET` is set to the string `"true"` (not `True` or `1`).
- Check that the raw JSON task ran successfully and its S3 key is in XCom.
- Ensure `pandas` and `pyarrow` are available:
  ```bash
  docker compose exec airflow-worker pip show pandas pyarrow
  ```

### Container health check fails

```bash
docker compose ps                    # identify the unhealthy container
docker compose logs <service-name>   # read the logs
```

Common cause: PostgreSQL or Redis not ready before Airflow starts. The `depends_on` health checks should handle this, but on slow machines you may need to retry `docker compose up -d`.

---

## Updating the pipeline

### Adding a new DAG

1. Create `dags/comtrade_<name>.py` following the pattern of any existing DAG.
2. Add the new endpoint function to `plugins/comtrade/client.py`.
3. The scheduler will pick up the new file on its next parse cycle (within ~30 seconds).

### Changing the Airflow version

1. Update `AIRFLOW_IMAGE_NAME` in `.env`.
2. Update the `apache-airflow` version in `requirements.txt`.
3. Run `docker compose pull && docker compose up -d`.

### Modifying Variables without a restart

All Variables are read at task runtime (not parse time), so changes take effect on the next DAG run with no restart needed.

---

---

## Production deployment (AWS MWAA)

### How it works

In `dev`, Docker Compose is the runtime. In `staging` and `prod`, AWS MWAA
(Managed Airflow) replaces it. The `deploy.yml` workflow handles the full
promotion pipeline:

```
main branch merge
    │
    ▼
build-push ──► ECR image (sha-<git-sha>)
    │
    ▼
deploy-dev  ──► terraform apply (enable_mwaa=false) — automatic
    │
    ▼ (manual approval via GitHub Environment "staging")
deploy-staging ──► terraform apply (enable_mwaa=true) + S3 artifact sync
    │
    ▼ (manual approval via GitHub Environment "prod")
deploy-prod ──► terraform apply (mw1.medium, max 10 workers) + S3 artifact sync
```

### First-time setup

**1. Create an OIDC role in AWS** so GitHub Actions can assume it without stored credentials:

```bash
# In AWS Console → IAM → Identity providers → Add provider
# Provider URL: https://token.actions.githubusercontent.com
# Audience: sts.amazonaws.com
# Then create a role that trusts this provider for your repo.
```

**2. Add GitHub repository secrets:**

| Secret | Value |
|--------|-------|
| `AWS_DEPLOY_ROLE_ARN` | ARN of the OIDC IAM role |
| `ECR_REPO_NAME` | `global-trade-<env>-airflow` |
| `TF_STATE_BUCKET` | S3 bucket for Terraform remote state |

**3. Configure GitHub Environments** (`Settings → Environments`):

| Environment | Protection |
|-------------|-----------|
| `dev` | None — deploys automatically |
| `staging` | Required reviewers (1+) |
| `prod` | Required reviewers (1+) + wait timer |

**4. Enable the Terraform S3 backend** by uncommenting the `backend "s3"` block in `terraform/main.tf`.

### MWAA environment classes

| Class | vCPU | Memory | Recommended for |
|-------|------|--------|----------------|
| `mw1.small` | 2 | 4 GB | staging (low volume) |
| `mw1.medium` | 4 | 8 GB | prod (≤ 8 DAGs, monthly schedule) |
| `mw1.large` | 8 | 16 GB | prod (high-frequency or many DAGs) |

### Updating DAGs in MWAA

The `deploy-staging` and `deploy-prod` jobs automatically sync `dags/`, zip `plugins/`, and upload `requirements.txt` to the MWAA artifacts bucket on every approved deploy.

To manually sync without a full Terraform apply:

```bash
BUCKET=$(terraform -chdir=terraform output -raw mwaa_artifacts_bucket)
aws s3 sync dags/ "s3://${BUCKET}/dags/" --delete
zip -r plugins.zip plugins/ && aws s3 cp plugins.zip "s3://${BUCKET}/plugins.zip"
aws s3 cp requirements.txt "s3://${BUCKET}/requirements.txt"
```

### Docker image (ECR)

Every merge to `main` builds `Dockerfile` and pushes a tagged image to ECR:

```
<account>.dkr.ecr.<region>.amazonaws.com/global-trade-<env>-airflow:sha-<git-sha>
```

Pull it locally for debugging:

```bash
aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin <ecr-url>
docker pull <ecr-url>:sha-<git-sha>
```

---

## Athena named queries

The Terraform configuration provisions a dedicated Athena workgroup and five pre-built named queries for common trade analyses.

### Workgroup

| Setting | Value |
|---------|-------|
| Name | `<project>-<env>-comtrade` |
| Result location | `s3://<bucket>/athena-results/` |
| Encryption | SSE-S3 |
| Byte-scan limit | 10 GB per query (cost guard) |
| CloudWatch metrics | Enabled |

### Named queries

| Query name | Description |
|------------|-------------|
| `top-exporters-by-period` | Top 20 countries by export value for a given year |
| `top-commodities-by-reporter` | Top 20 commodities for a reporting country |
| `bilateral-trade-balance` | Import vs export trend between two countries |
| `yoy-growth-by-reporter` | Year-over-year export/import growth rates |
| `data-freshness-check` | Latest available period per reporter — verify pipeline health |

### Running a named query

```bash
# List saved query IDs
aws athena list-named-queries --work-group <workgroup-name>

# Get query SQL
aws athena get-named-query --named-query-id <id>

# Start execution
aws athena start-query-execution \
  --query-string "$(aws athena get-named-query --named-query-id <id> --query 'NamedQuery.QueryString' --output text)" \
  --work-group <workgroup-name>
```

Or open the **Athena console → Saved queries** tab, select the workgroup, and click **Run**.

### After `terraform apply`

The `athena_named_queries` output maps query names to their IDs:

```bash
terraform output athena_named_queries
```

---

## QuickSight dashboards

QuickSight provides a managed BI layer on top of the silver Iceberg tables (queried via Athena).  All resources are Terraform-provisioned and gated on a feature flag so they are not deployed by default.

### Enabling QuickSight

```bash
terraform apply \
  -var="enable_quicksight=true" \
  -var="quicksight_username=<your-qs-user>"
```

`quicksight_username` is the short username shown in **QuickSight → Manage QuickSight → Users** (not the email address).

After `apply`:

```bash
terraform output quicksight_console_url   # open this URL to build analyses
terraform output quicksight_data_source_arn
```

### Provisioned resources

| Resource | Name / ID | Purpose |
|----------|-----------|---------|
| IAM role | `<prefix>-quicksight` | Service role — QuickSight assumes this to access Athena, S3, and Glue |
| IAM policy | `comtrade-data-access` | Inline policy granting least-privilege data lake access |
| Data source | `<prefix>-athena` | Athena connection via the project workgroup |
| Dataset | `<prefix>-reporter-summary` | SPICE import — per-country annual trade totals |
| Dataset | `<prefix>-trade-flows` | SPICE import — commodity-level bilateral flows |

### Datasets

Both datasets use **SPICE** (in-memory cache) for sub-second dashboard rendering and filter to **annual data** (`freq_code = 'A'`).  Monthly data can be queried live by changing `import_mode = "DIRECT_QUERY"` in `terraform/quicksight.tf`.

| Dataset | Source table | Rows | Key columns |
|---------|-------------|------|-------------|
| Reporter Summary | `comtrade_silver.reporter_summary` | ~200 × years | `reporter_iso`, `export_value_usd`, `trade_balance_usd` |
| Trade Flows | `comtrade_silver.trade_flows` | millions × years | `reporter_iso`, `partner_iso`, `commodity_code`, `trade_value_usd` |

### Building analyses

1. Open the **QuickSight console** (`terraform output quicksight_console_url`).
2. Go to **Datasets** — you will see `Comtrade Reporter Summary` and `Comtrade Trade Flows`.
3. Click a dataset → **Create analysis**.
4. Suggested starting visuals:
   - **Bar chart** — `reporter_name` × `export_value_usd` (filter to a single `period`)
   - **Line chart** — `period` × `total_trade_value_usd` grouped by `reporter_iso`
   - **Heat map** — `reporter_iso` × `partner_iso` coloured by `trade_value_usd`

### Refreshing SPICE data

SPICE must be refreshed after each pipeline run (Airflow does not trigger this automatically):

```bash
# Using the AWS CLI
aws quicksight create-ingestion \
  --aws-account-id <account-id> \
  --data-set-id <prefix>-reporter-summary \
  --ingestion-id manual-$(date +%Y%m%d%H%M%S)
```

Or in the console: **Datasets → <dataset> → Refresh → Full refresh**.

---

## IAM policy (minimum required)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject"
      ],
      "Resource": "arn:aws:s3:::<COMTRADE_S3_BUCKET>/comtrade/*"
    }
  ]
}
```
