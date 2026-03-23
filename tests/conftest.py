"""
Root conftest.py

Responsibilities:
1. Extend sys.path so ``from comtrade import ...`` works without installing.
2. Set Airflow environment variables before any import (harmless when Airflow
   is absent).
3. Provide fixtures that are safe to use regardless of whether Airflow is
   installed: ``sample_api_response``.

Airflow-specific fixtures (``mock_variables``, DB init) live in sub-package
conftest files so that pure-Python tests (test_client.py) are never blocked.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

# ── 1. Path setup ─────────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(REPO_ROOT / "plugins"))
sys.path.insert(0, str(REPO_ROOT / "dags"))

# ── 2. Airflow minimal config (must precede any `import airflow`) ─────────────
_TEST_DB = f"sqlite:///{REPO_ROOT}/tests/.airflow_test.db"
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
os.environ.setdefault("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", _TEST_DB)
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault(
    "AIRFLOW__CORE__FERNET_KEY",
    "J5NfuI5BqxPNgQe5xFqBb_eJGG-FJZxcQy5IXPQ6QqM=",
)
os.environ.setdefault("AIRFLOW_HOME", str(REPO_ROOT / "tests" / ".airflow_home"))

# ── 3. Shared fixtures (no Airflow dependency) ────────────────────────────────
import pytest  # noqa: E402


@pytest.fixture
def sample_api_response():
    """Typical Comtrade API response envelope with two trade records."""
    return {
        "elapsedMs": 42,
        "count": 2,
        "data": [
            {
                "typeCode": "C",
                "freqCode": "A",
                "refPeriodId": 2023,
                "reporterCode": "840",
                "reporterDesc": "USA",
                "partnerCode": "276",
                "partnerDesc": "Germany",
                "cmdCode": "TOTAL",
                "primaryValue": 1_234_567.89,
            },
            {
                "typeCode": "C",
                "freqCode": "A",
                "refPeriodId": 2023,
                "reporterCode": "276",
                "reporterDesc": "Germany",
                "partnerCode": "840",
                "partnerDesc": "USA",
                "cmdCode": "TOTAL",
                "primaryValue": 987_654.32,
            },
        ],
    }
