"""
Unit tests for dags/comtrade_iceberg_maintenance.py

Validates the structural properties of the weekly Iceberg maintenance DAG
without requiring a live Airflow scheduler. AWS calls are mocked.
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("airflow", reason="apache-airflow not installed")

pytestmark = pytest.mark.unit

_DAGS_DIR = Path(__file__).parents[2] / "dags"
_PLUGINS_DIR = Path(__file__).parents[2] / "plugins"
for _p in (_PLUGINS_DIR, _DAGS_DIR):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import comtrade_iceberg_maintenance as _mod  # noqa: E402


# ── DAG structure ─────────────────────────────────────────────────────────────


class TestDagStructure:
    def test_dag_id(self):
        assert _mod.dag.dag_id == "comtrade_iceberg_maintenance"

    def test_dag_runs_weekly_on_sunday_04utc(self):
        assert _mod.dag.schedule_interval == "0 4 * * 0"

    def test_catchup_disabled(self):
        assert _mod.dag.catchup is False

    def test_has_both_maintenance_tasks(self):
        ids = {t.task_id for t in _mod.dag.tasks}
        assert ids == {"maintain_bronze", "maintain_silver"}

    def test_silver_runs_after_bronze(self):
        bronze = _mod.dag.get_task("maintain_bronze")
        silver = _mod.dag.get_task("maintain_silver")
        assert "maintain_silver" in {t.task_id for t in bronze.downstream_list}
        assert "maintain_bronze" in {t.task_id for t in silver.upstream_list}

    def test_failure_callback_attached(self):
        for task in _mod.dag.tasks:
            assert task.on_failure_callback is not None

    def test_sla_miss_callback_attached(self):
        assert _mod.dag.sla_miss_callback is not None

    def test_tags_present(self):
        assert {"comtrade", "iceberg", "maintenance"}.issubset(set(_mod.dag.tags))


# ── Table coverage ────────────────────────────────────────────────────────────


class TestTableCoverage:
    def test_bronze_includes_all_endpoints(self):
        expected = {
            "preview",
            "previewTariffline",
            "getMBS",
            "getDA",
            "getDATariffline",
            "getWorldShare",
            "getMetadata",
            "getComtradeReleases",
        }
        assert expected == set(_mod.BRONZE_TABLES)

    def test_silver_includes_published_tables(self):
        assert set(_mod.SILVER_TABLES) == {"reporter_summary", "trade_flows"}


# ── _maintain_tables behaviour ────────────────────────────────────────────────


class TestMaintainTables:
    def test_runs_optimize_and_vacuum_for_each_table(self):
        with (
            patch.object(_mod, "_run_athena_statement") as run,
            patch("airflow.models.Variable.get") as varget,
        ):
            varget.side_effect = lambda key, default_var=None: {
                "ATHENA_WORKGROUP": "wg",
                "ATHENA_OUTPUT_LOCATION": "s3://bucket/results/",
                "AWS_DEFAULT_REGION": "us-east-1",
            }.get(key, default_var)
            _mod._maintain_tables("comtrade", ["preview", "getMBS"])

        statements = [c.args[0] for c in run.call_args_list]
        # Two operations per table → 4 statements
        assert len(statements) == 4
        assert any("OPTIMIZE" in s and "preview" in s for s in statements)
        assert any("VACUUM" in s and "preview" in s for s in statements)
        assert any("OPTIMIZE" in s and "getMBS" in s for s in statements)
        assert any("VACUUM" in s and "getMBS" in s for s in statements)

    def test_table_names_quoted_to_preserve_case(self):
        with (
            patch.object(_mod, "_run_athena_statement") as run,
            patch("airflow.models.Variable.get") as varget,
        ):
            varget.side_effect = lambda key, default_var=None: {
                "ATHENA_WORKGROUP": "wg",
                "ATHENA_OUTPUT_LOCATION": "s3://bucket/results/",
            }.get(key, default_var)
            _mod._maintain_tables("comtrade", ["previewTariffline"])

        statements = [c.args[0] for c in run.call_args_list]
        for s in statements:
            assert '"comtrade"."previewTariffline"' in s

    def test_collects_failures_and_raises_once(self):
        with (
            patch.object(_mod, "_run_athena_statement", side_effect=RuntimeError("nope")),
            patch("airflow.models.Variable.get") as varget,
            pytest.raises(RuntimeError, match="2 failure"),
        ):
            varget.side_effect = lambda key, default_var=None: {
                "ATHENA_WORKGROUP": "wg",
                "ATHENA_OUTPUT_LOCATION": "s3://bucket/results/",
            }.get(key, default_var)
            _mod._maintain_tables("comtrade", ["preview"])  # 2 ops fail → 2 failures


# ── _run_athena_statement polling ─────────────────────────────────────────────


class TestRunAthenaStatement:
    def _client(self, states):
        client = MagicMock()
        client.start_query_execution.return_value = {"QueryExecutionId": "q-1"}
        client.get_query_execution.side_effect = [
            {"QueryExecution": {"Status": {"State": s, "StateChangeReason": "x"}}}
            for s in states
        ]
        return client

    def test_success_after_a_few_polls(self):
        client = self._client(["RUNNING", "RUNNING", "SUCCEEDED"])
        with (
            patch.object(_mod, "_athena_client", return_value=client),
            patch("comtrade_iceberg_maintenance.time.sleep"),
        ):
            _mod._run_athena_statement("VACUUM t", "wg", "s3://b/", "us-east-1")
        assert client.start_query_execution.called

    def test_failed_state_raises(self):
        client = self._client(["FAILED"])
        with (
            patch.object(_mod, "_athena_client", return_value=client),
            patch("comtrade_iceberg_maintenance.time.sleep"),
            pytest.raises(RuntimeError, match="FAILED"),
        ):
            _mod._run_athena_statement("VACUUM t", "wg", "s3://b/", "us-east-1")
