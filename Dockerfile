# ── Comtrade Pipeline — Custom Airflow Image ──────────────────────────────────
#
# Used for:
#   - Local development (docker compose up) with pinned dependencies
#   - ECR / ECS production deployment
#
# For MWAA deployments, requirements.txt is uploaded to S3 instead of baked in.
# ─────────────────────────────────────────────────────────────────────────────
FROM apache/airflow:2.9.3-python3.11

# Install pipeline dependencies.
# requirements.txt is also uploaded to MWAA via CI — keep them in sync.
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy the plugins package into the image so it is available without mounting.
# When running locally with Docker Compose the ./plugins volume takes precedence.
COPY --chown=airflow:root plugins/ /opt/airflow/plugins/
