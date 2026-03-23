"""
DAG integrity tests

Verifies that all 10 Comtrade DAGs (8 ingestion + 1 dbt + 1 backfill):
  1. Parse and import without errors
  2. Have the expected dag_id, schedule, tags, and catchup setting
  3. Contain exactly the expected tasks with correct task IDs
  4. Have no dependency cycles

These tests use Airflow's DagBag — the same mechanism the scheduler uses to
load DAGs — so they catch import errors, misconfigured operators, and broken
dependency chains before they reach production.

``airflow.models.Variable.get`` is patched to return sensible defaults so the
DAGs can be loaded without a populated Airflow Variable store.
"""
from __future__ import annotations

from pathlib import Path
from unittest import mock

import pytest

# Skip this entire module if Airflow is not installed
pytest.importorskip("airflow", reason="apache-airflow not installed")

pytestmark = pytest.mark.dag_integrity

DAGS_DIR = Path(__file__).parent.parent.parent / "dags"

# ── Expected DAG metadata ─────────────────────────────────────────────────────

# Ingestion-only DAGs (extract → validate → parquet).
INGESTION_DAGS: dict[str, dict] = {
    "comtrade_preview": {
        "schedule": "@monthly",
        "tags": {"comtrade", "preview", "s3"},
        "task_ids": {"extract_and_store_raw", "validate_bronze", "convert_to_parquet"},
        "catchup": False,
    },
    "comtrade_preview_tariffline": {
        "schedule": "@monthly",
        "tags": {"comtrade", "tariffline", "s3"},
        "task_ids": {"extract_and_store_raw", "validate_bronze", "convert_to_parquet"},
        "catchup": False,
    },
    "comtrade_world_share": {
        "schedule": "@monthly",
        "tags": {"comtrade", "world-share", "s3"},
        "task_ids": {"extract_and_store_raw", "validate_bronze", "convert_to_parquet"},
        "catchup": False,
    },
    "comtrade_metadata": {
        "schedule": "@weekly",
        "tags": {"comtrade", "metadata", "s3"},
        "task_ids": {"extract_and_store_raw", "validate_bronze", "convert_to_parquet"},
        "catchup": False,
    },
    "comtrade_mbs": {
        "schedule": "@monthly",
        "tags": {"comtrade", "mbs", "s3"},
        "task_ids": {"extract_and_store_raw", "validate_bronze", "convert_to_parquet"},
        "catchup": False,
    },
    "comtrade_da_tariffline": {
        "schedule": "@monthly",
        "tags": {"comtrade", "da", "tariffline", "s3"},
        "task_ids": {"extract_and_store_raw", "validate_bronze", "convert_to_parquet"},
        "catchup": False,
    },
    "comtrade_da": {
        "schedule": "@monthly",
        "tags": {"comtrade", "da", "s3"},
        "task_ids": {"extract_and_store_raw", "validate_bronze", "convert_to_parquet"},
        "catchup": False,
    },
    "comtrade_releases": {
        "schedule": "@daily",
        "tags": {"comtrade", "releases", "s3"},
        "task_ids": {"extract_and_store_raw", "validate_bronze", "convert_to_parquet"},
        "catchup": False,
    },
}

# All DAGs — ingestion + dbt transformation + backfill.
EXPECTED_DAGS: dict[str, dict] = {
    **INGESTION_DAGS,
    "comtrade_dbt": {
        "schedule": "@monthly",
        "tags": {"comtrade", "dbt", "silver"},
        "task_ids": {
            "dbt_deps",
            "dbt_source_freshness",
            "dbt_run_staging",
            "dbt_run_silver",
            "dbt_test",
        },
        "catchup": False,
    },
    "comtrade_backfill": {
        "schedule": None,   # manually triggered only
        "tags": {"comtrade", "backfill"},
        "task_ids": {"validate_conf", "run_backfill"},
        "catchup": False,
    },
}


# ── Fixture: load the DagBag once per module ──────────────────────────────────


@pytest.fixture(scope="module")
def dagbag():
    """
    Load all DAGs using Airflow's DagBag with Variable.get patched so that
    parse-time ``Variable.get(...)`` calls return their ``default_var``.
    """
    from airflow.models import DagBag

    def _fake_variable_get(key, default_var=None, **kwargs):
        return default_var

    with mock.patch("airflow.models.Variable.get", side_effect=_fake_variable_get):
        bag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)

    return bag


# ── Tests ─────────────────────────────────────────────────────────────────────


class TestDagBagLoads:
    def test_no_import_errors(self, dagbag):
        """No DAG file must produce an import error."""
        errors = dagbag.import_errors
        assert errors == {}, f"DagBag import errors:\n{errors}"

    def test_all_expected_dags_are_present(self, dagbag):
        """Every expected dag_id must be registered in the DagBag."""
        missing = set(EXPECTED_DAGS) - set(dagbag.dags)
        assert not missing, f"Missing DAGs: {missing}"

    def test_no_unexpected_dags(self, dagbag):
        """There must be no extra DAGs we didn't intend to ship."""
        unexpected = set(dagbag.dags) - set(EXPECTED_DAGS)
        assert not unexpected, f"Unexpected DAGs found: {unexpected}"


class TestDagSchedule:
    @pytest.mark.parametrize("dag_id,meta", EXPECTED_DAGS.items())
    def test_schedule_interval(self, dagbag, dag_id, meta):
        dag = dagbag.get_dag(dag_id)
        if meta["schedule"] is None:
            # Manually-triggered DAGs (schedule=None) have no timetable.
            assert dag.schedule_interval is None, (
                f"{dag_id}: expected schedule=None (manually triggered), "
                f"got '{dag.schedule_interval}'"
            )
        else:
            # Airflow 2.4+ stores timetable; schedule_interval is kept for compatibility.
            actual = dag.schedule_interval or str(dag.timetable)
            assert actual == meta["schedule"], (
                f"{dag_id}: expected schedule '{meta['schedule']}', got '{actual}'"
            )


class TestDagConfig:
    @pytest.mark.parametrize("dag_id,meta", EXPECTED_DAGS.items())
    def test_catchup_is_false(self, dagbag, dag_id, meta):
        dag = dagbag.get_dag(dag_id)
        assert dag.catchup is False, f"{dag_id}: catchup should be False"

    @pytest.mark.parametrize("dag_id,meta", EXPECTED_DAGS.items())
    def test_tags(self, dagbag, dag_id, meta):
        dag = dagbag.get_dag(dag_id)
        assert set(dag.tags) == meta["tags"], (
            f"{dag_id}: expected tags {meta['tags']}, got {set(dag.tags)}"
        )

    @pytest.mark.parametrize("dag_id", EXPECTED_DAGS)
    def test_has_description(self, dagbag, dag_id):
        dag = dagbag.get_dag(dag_id)
        assert dag.description, f"{dag_id}: description should not be empty"

    @pytest.mark.parametrize("dag_id", EXPECTED_DAGS)
    def test_default_args_retries(self, dagbag, dag_id):
        dag = dagbag.get_dag(dag_id)
        retries = dag.default_args.get("retries")
        assert retries is not None and retries >= 1, (
            f"{dag_id}: default_args.retries should be >= 1"
        )

    @pytest.mark.parametrize("dag_id", EXPECTED_DAGS)
    def test_start_date_is_set(self, dagbag, dag_id):
        dag = dagbag.get_dag(dag_id)
        assert dag.start_date is not None, f"{dag_id}: start_date must be set"


class TestDagTasks:
    @pytest.mark.parametrize("dag_id,meta", EXPECTED_DAGS.items())
    def test_task_count(self, dagbag, dag_id, meta):
        dag = dagbag.get_dag(dag_id)
        assert len(dag.tasks) == len(meta["task_ids"]), (
            f"{dag_id}: expected {len(meta['task_ids'])} tasks, "
            f"got {len(dag.tasks)} ({[t.task_id for t in dag.tasks]})"
        )

    @pytest.mark.parametrize("dag_id,meta", EXPECTED_DAGS.items())
    def test_task_ids(self, dagbag, dag_id, meta):
        dag = dagbag.get_dag(dag_id)
        actual_ids = {t.task_id for t in dag.tasks}
        assert actual_ids == meta["task_ids"], (
            f"{dag_id}: expected task IDs {meta['task_ids']}, got {actual_ids}"
        )

    @pytest.mark.parametrize("dag_id", INGESTION_DAGS)
    def test_extract_task_exists(self, dagbag, dag_id):
        dag = dagbag.get_dag(dag_id)
        assert dag.has_task("extract_and_store_raw"), (
            f"{dag_id}: missing task 'extract_and_store_raw'"
        )

    @pytest.mark.parametrize("dag_id", INGESTION_DAGS)
    def test_parquet_task_exists(self, dagbag, dag_id):
        dag = dagbag.get_dag(dag_id)
        assert dag.has_task("convert_to_parquet"), (
            f"{dag_id}: missing task 'convert_to_parquet'"
        )

    @pytest.mark.parametrize("dag_id", INGESTION_DAGS)
    def test_validate_task_exists(self, dagbag, dag_id):
        dag = dagbag.get_dag(dag_id)
        assert dag.has_task("validate_bronze"), (
            f"{dag_id}: missing task 'validate_bronze'"
        )

    @pytest.mark.parametrize("dag_id", INGESTION_DAGS)
    def test_validate_depends_on_extract(self, dagbag, dag_id):
        """validate_bronze must have extract_and_store_raw as upstream."""
        dag = dagbag.get_dag(dag_id)
        validate_task = dag.get_task("validate_bronze")
        upstream_ids = {t.task_id for t in validate_task.upstream_list}
        assert "extract_and_store_raw" in upstream_ids, (
            f"{dag_id}: 'validate_bronze' must depend on 'extract_and_store_raw'"
        )

    @pytest.mark.parametrize("dag_id", INGESTION_DAGS)
    def test_parquet_depends_on_validate(self, dagbag, dag_id):
        """convert_to_parquet must have validate_bronze as upstream."""
        dag = dagbag.get_dag(dag_id)
        parquet_task = dag.get_task("convert_to_parquet")
        upstream_ids = {t.task_id for t in parquet_task.upstream_list}
        assert "validate_bronze" in upstream_ids, (
            f"{dag_id}: 'convert_to_parquet' must depend on 'validate_bronze'"
        )

    @pytest.mark.parametrize("dag_id", EXPECTED_DAGS)
    def test_no_cycles(self, dagbag, dag_id):
        """Airflow raises on cycles during DagBag loading; this is a belt-and-suspenders check."""
        dag = dagbag.get_dag(dag_id)
        # DagBag would have raised on import if a cycle existed; verify topological sort works
        try:
            dag.topological_sort()
        except Exception as exc:
            pytest.fail(f"{dag_id}: topological sort failed (cycle?): {exc}")


class TestDagFiles:
    def test_all_dag_files_exist(self):
        """Every expected DAG must have a corresponding file on disk."""
        dag_id_to_file = {
            "comtrade_preview": "comtrade_preview.py",
            "comtrade_preview_tariffline": "comtrade_preview_tariffline.py",
            "comtrade_world_share": "comtrade_world_share.py",
            "comtrade_metadata": "comtrade_metadata.py",
            "comtrade_mbs": "comtrade_mbs.py",
            "comtrade_da_tariffline": "comtrade_da_tariffline.py",
            "comtrade_da": "comtrade_da.py",
            "comtrade_releases": "comtrade_releases.py",
            "comtrade_dbt": "comtrade_dbt.py",
            "comtrade_backfill": "comtrade_backfill.py",
        }
        for dag_id, filename in dag_id_to_file.items():
            path = DAGS_DIR / filename
            assert path.exists(), f"DAG file missing: {path}"

    def test_plugin_package_has_init(self):
        plugins_init = DAGS_DIR.parent / "plugins" / "comtrade" / "__init__.py"
        assert plugins_init.exists(), f"Plugin __init__.py missing: {plugins_init}"


class TestDbtDag:
    """Structural tests specific to the comtrade_dbt transformation DAG."""

    def test_dbt_dag_is_present(self, dagbag):
        assert "comtrade_dbt" in dagbag.dags, "comtrade_dbt DAG is missing"

    def test_has_five_tasks(self, dagbag):
        dag = dagbag.get_dag("comtrade_dbt")
        assert len(dag.tasks) == 5, (
            f"Expected 5 dbt tasks, got {len(dag.tasks)}: {[t.task_id for t in dag.tasks]}"
        )

    def test_dbt_deps_task_exists(self, dagbag):
        dag = dagbag.get_dag("comtrade_dbt")
        assert dag.has_task("dbt_deps")

    def test_dbt_source_freshness_task_exists(self, dagbag):
        dag = dagbag.get_dag("comtrade_dbt")
        assert dag.has_task("dbt_source_freshness")

    def test_dbt_run_staging_task_exists(self, dagbag):
        dag = dagbag.get_dag("comtrade_dbt")
        assert dag.has_task("dbt_run_staging")

    def test_dbt_run_silver_task_exists(self, dagbag):
        dag = dagbag.get_dag("comtrade_dbt")
        assert dag.has_task("dbt_run_silver")

    def test_dbt_test_task_exists(self, dagbag):
        dag = dagbag.get_dag("comtrade_dbt")
        assert dag.has_task("dbt_test")

    def test_source_freshness_depends_on_deps(self, dagbag):
        dag = dagbag.get_dag("comtrade_dbt")
        upstream_ids = {t.task_id for t in dag.get_task("dbt_source_freshness").upstream_list}
        assert "dbt_deps" in upstream_ids

    def test_run_staging_depends_on_source_freshness(self, dagbag):
        dag = dagbag.get_dag("comtrade_dbt")
        upstream_ids = {t.task_id for t in dag.get_task("dbt_run_staging").upstream_list}
        assert "dbt_source_freshness" in upstream_ids

    def test_run_silver_depends_on_staging(self, dagbag):
        dag = dagbag.get_dag("comtrade_dbt")
        upstream_ids = {t.task_id for t in dag.get_task("dbt_run_silver").upstream_list}
        assert "dbt_run_staging" in upstream_ids

    def test_dbt_test_depends_on_silver(self, dagbag):
        dag = dagbag.get_dag("comtrade_dbt")
        upstream_ids = {t.task_id for t in dag.get_task("dbt_test").upstream_list}
        assert "dbt_run_silver" in upstream_ids

    def test_no_cycles(self, dagbag):
        dag = dagbag.get_dag("comtrade_dbt")
        try:
            dag.topological_sort()
        except Exception as exc:
            pytest.fail(f"comtrade_dbt: topological sort failed (cycle?): {exc}")


class TestBackfillDag:
    """Structural tests for the comtrade_backfill manually-triggered DAG."""

    def test_backfill_dag_is_present(self, dagbag):
        assert "comtrade_backfill" in dagbag.dags, "comtrade_backfill DAG is missing"

    def test_schedule_is_none(self, dagbag):
        dag = dagbag.get_dag("comtrade_backfill")
        assert dag.schedule_interval is None, (
            "comtrade_backfill must have schedule=None (manually triggered)"
        )

    def test_catchup_is_false(self, dagbag):
        dag = dagbag.get_dag("comtrade_backfill")
        assert dag.catchup is False

    def test_has_two_tasks(self, dagbag):
        dag = dagbag.get_dag("comtrade_backfill")
        assert len(dag.tasks) == 2, (
            f"Expected 2 tasks, got {len(dag.tasks)}: {[t.task_id for t in dag.tasks]}"
        )

    def test_validate_conf_task_exists(self, dagbag):
        dag = dagbag.get_dag("comtrade_backfill")
        assert dag.has_task("validate_conf")

    def test_run_backfill_task_exists(self, dagbag):
        dag = dagbag.get_dag("comtrade_backfill")
        assert dag.has_task("run_backfill")

    def test_run_backfill_depends_on_validate_conf(self, dagbag):
        dag = dagbag.get_dag("comtrade_backfill")
        upstream_ids = {t.task_id for t in dag.get_task("run_backfill").upstream_list}
        assert "validate_conf" in upstream_ids

    def test_has_description(self, dagbag):
        dag = dagbag.get_dag("comtrade_backfill")
        assert dag.description

    def test_tags_include_backfill(self, dagbag):
        dag = dagbag.get_dag("comtrade_backfill")
        assert "backfill" in dag.tags

    def test_no_cycles(self, dagbag):
        dag = dagbag.get_dag("comtrade_backfill")
        try:
            dag.topological_sort()
        except Exception as exc:
            pytest.fail(f"comtrade_backfill: topological sort failed (cycle?): {exc}")
