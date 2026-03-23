"""
Unit tests for the dbt silver layer project structure.

These tests validate the project configuration without installing dbt,
dbt-athena, or connecting to AWS.  They parse YAML files directly and
verify that every model referenced in schema files has a matching SQL file,
that sources are well-formed, and that required test coverage is in place.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml

# Root of the dbt project relative to the repo root.
DBT_ROOT = Path(__file__).parents[2] / "dbt"
MODELS_DIR = DBT_ROOT / "models"
TESTS_DIR = DBT_ROOT / "tests"


# ── Helpers ───────────────────────────────────────────────────────────────────


def _load_yaml(path: Path) -> dict:
    with path.open() as f:
        return yaml.safe_load(f)


def _model_sql_files() -> dict[str, Path]:
    """Return {model_name: path} for every .sql file under models/."""
    return {p.stem: p for p in MODELS_DIR.rglob("*.sql")}


def _schema_model_names() -> list[str]:
    """Return every model name declared in *__models.yml files."""
    names = []
    for yml in MODELS_DIR.rglob("*__models.yml"):
        doc = _load_yaml(yml)
        for model in doc.get("models", []):
            names.append(model["name"])
    return names


# ── dbt_project.yml ───────────────────────────────────────────────────────────


class TestDbtProjectYml:
    def setup_method(self):
        self.doc = _load_yaml(DBT_ROOT / "dbt_project.yml")

    def test_name_is_comtrade(self):
        assert self.doc["name"] == "comtrade"

    def test_version_is_present(self):
        assert "version" in self.doc

    def test_profile_is_comtrade(self):
        assert self.doc["profile"] == "comtrade"

    def test_model_paths_include_models(self):
        assert "models" in self.doc.get("model-paths", [])

    def test_test_paths_include_tests(self):
        assert "tests" in self.doc.get("test-paths", [])

    def test_staging_models_materialized_as_view(self):
        staging = self.doc["models"]["comtrade"]["staging"]
        assert staging["+materialized"] == "view"

    def test_silver_models_materialized_as_table(self):
        silver = self.doc["models"]["comtrade"]["silver"]
        assert silver["+materialized"] == "table"

    def test_silver_uses_iceberg_format(self):
        silver = self.doc["models"]["comtrade"]["silver"]
        assert silver["+table_type"] == "iceberg"

    def test_silver_partitioned_by_period(self):
        silver = self.doc["models"]["comtrade"]["silver"]
        assert "period" in silver["+partitioned_by"]

    def test_clean_targets_include_target_and_packages(self):
        clean = self.doc.get("clean-targets", [])
        assert "target" in clean
        assert "dbt_packages" in clean


# ── profiles.yml ─────────────────────────────────────────────────────────────


class TestProfilesYml:
    def setup_method(self):
        self.doc = _load_yaml(DBT_ROOT / "profiles.yml")

    def test_comtrade_profile_exists(self):
        assert "comtrade" in self.doc

    def test_dev_target_exists(self):
        assert "dev" in self.doc["comtrade"]["outputs"]

    def test_prod_target_exists(self):
        assert "prod" in self.doc["comtrade"]["outputs"]

    def test_dev_type_is_athena(self):
        assert self.doc["comtrade"]["outputs"]["dev"]["type"] == "athena"

    def test_dev_database_is_comtrade(self):
        assert self.doc["comtrade"]["outputs"]["dev"]["database"] == "comtrade"

    def test_dev_schema_is_silver(self):
        assert self.doc["comtrade"]["outputs"]["dev"]["schema"] == "silver"

    def test_dev_uses_env_var_for_bucket(self):
        s3_dir = self.doc["comtrade"]["outputs"]["dev"]["s3_staging_dir"]
        assert "env_var" in s3_dir and "COMTRADE_S3_BUCKET" in s3_dir


# ── packages.yml ─────────────────────────────────────────────────────────────


class TestPackagesYml:
    def setup_method(self):
        self.doc = _load_yaml(DBT_ROOT / "packages.yml")

    def test_dbt_utils_is_declared(self):
        packages = [p["package"] for p in self.doc.get("packages", [])]
        assert "dbt-labs/dbt_utils" in packages


# ── sources.yml ──────────────────────────────────────────────────────────────


class TestSourcesYml:
    def setup_method(self):
        self.doc = _load_yaml(MODELS_DIR / "sources.yml")

    def test_version_is_2(self):
        assert self.doc["version"] == 2

    def test_bronze_source_exists(self):
        names = [s["name"] for s in self.doc["sources"]]
        assert "bronze" in names

    def test_bronze_database_is_comtrade(self):
        bronze = next(s for s in self.doc["sources"] if s["name"] == "bronze")
        assert bronze["database"] == "comtrade"

    def test_preview_table_declared(self):
        bronze = next(s for s in self.doc["sources"] if s["name"] == "bronze")
        table_names = [t["name"] for t in bronze["tables"]]
        assert "preview" in table_names

    def test_getmbs_table_declared(self):
        bronze = next(s for s in self.doc["sources"] if s["name"] == "bronze")
        table_names = [t["name"] for t in bronze["tables"]]
        assert "getmbs" in table_names

    def test_preview_has_not_null_tests_on_required_columns(self):
        bronze = next(s for s in self.doc["sources"] if s["name"] == "bronze")
        preview = next(t for t in bronze["tables"] if t["name"] == "preview")
        columns_with_not_null = []
        for col in preview.get("columns", []):
            tests = col.get("tests", [])
            if "not_null" in tests:
                columns_with_not_null.append(col["name"])
        assert "reportercode" in columns_with_not_null
        assert "period" in columns_with_not_null


# ── Staging models ────────────────────────────────────────────────────────────


class TestStagingModels:
    def test_stg_preview_sql_exists(self):
        assert (MODELS_DIR / "staging" / "stg_preview.sql").exists()

    def test_stg_mbs_sql_exists(self):
        assert (MODELS_DIR / "staging" / "stg_mbs.sql").exists()

    def test_stg_preview_references_source(self):
        sql = (MODELS_DIR / "staging" / "stg_preview.sql").read_text()
        assert "source('bronze', 'preview')" in sql

    def test_stg_mbs_references_source(self):
        sql = (MODELS_DIR / "staging" / "stg_mbs.sql").read_text()
        assert "source('bronze', 'getmbs')" in sql

    def test_stg_preview_casts_trade_value(self):
        sql = (MODELS_DIR / "staging" / "stg_preview.sql").read_text()
        assert "trade_value_usd" in sql
        assert "double" in sql.lower() or "float" in sql.lower() or "cast" in sql.lower()

    def test_stg_preview_filters_null_reporter(self):
        sql = (MODELS_DIR / "staging" / "stg_preview.sql").read_text()
        assert "reportercode is not null" in sql.lower()

    def test_stg_schema_declares_both_models(self):
        doc = _load_yaml(MODELS_DIR / "staging" / "_stg__models.yml")
        model_names = [m["name"] for m in doc.get("models", [])]
        assert "stg_preview" in model_names
        assert "stg_mbs" in model_names


# ── Silver models ─────────────────────────────────────────────────────────────


class TestSilverModels:
    def test_trade_flows_sql_exists(self):
        assert (MODELS_DIR / "silver" / "trade_flows.sql").exists()

    def test_reporter_summary_sql_exists(self):
        assert (MODELS_DIR / "silver" / "reporter_summary.sql").exists()

    def test_trade_flows_references_stg_preview(self):
        sql = (MODELS_DIR / "silver" / "trade_flows.sql").read_text()
        assert "ref('stg_preview')" in sql

    def test_reporter_summary_references_stg_preview(self):
        sql = (MODELS_DIR / "silver" / "reporter_summary.sql").read_text()
        assert "ref('stg_preview')" in sql

    def test_trade_flows_aggregates_trade_value(self):
        sql = (MODELS_DIR / "silver" / "trade_flows.sql").read_text()
        assert "sum(trade_value_usd)" in sql.lower() or "sum(" in sql.lower()

    def test_reporter_summary_splits_imports_exports(self):
        sql = (MODELS_DIR / "silver" / "reporter_summary.sql").read_text()
        assert "export_value_usd" in sql
        assert "import_value_usd" in sql

    def test_reporter_summary_computes_balance(self):
        sql = (MODELS_DIR / "silver" / "reporter_summary.sql").read_text()
        assert "trade_balance_usd" in sql

    def test_silver_schema_declares_both_models(self):
        doc = _load_yaml(MODELS_DIR / "silver" / "_silver__models.yml")
        model_names = [m["name"] for m in doc.get("models", [])]
        assert "trade_flows" in model_names
        assert "reporter_summary" in model_names

    def test_trade_flows_schema_has_not_null_tests(self):
        doc = _load_yaml(MODELS_DIR / "silver" / "_silver__models.yml")
        trade_flows = next(m for m in doc["models"] if m["name"] == "trade_flows")
        cols_with_not_null = []
        for col in trade_flows.get("columns", []):
            if "not_null" in col.get("tests", []):
                cols_with_not_null.append(col["name"])
        assert "reporter_code" in cols_with_not_null
        assert "period" in cols_with_not_null

    def test_reporter_summary_schema_has_not_null_tests(self):
        doc = _load_yaml(MODELS_DIR / "silver" / "_silver__models.yml")
        summary = next(m for m in doc["models"] if m["name"] == "reporter_summary")
        cols_with_not_null = []
        for col in summary.get("columns", []):
            if "not_null" in col.get("tests", []):
                cols_with_not_null.append(col["name"])
        assert "reporter_code" in cols_with_not_null
        assert "period" in cols_with_not_null


# ── Schema coverage: every model file has a YAML entry ────────────────────────


class TestSchemaCoverage:
    def test_every_sql_model_has_schema_entry(self):
        sql_models = _model_sql_files()
        schema_models = set(_schema_model_names())
        missing = [name for name in sql_models if name not in schema_models]
        assert not missing, f"SQL models without schema entries: {missing}"

    def test_every_schema_entry_has_sql_file(self):
        sql_models = _model_sql_files()
        schema_models = _schema_model_names()
        missing = [name for name in schema_models if name not in sql_models]
        assert not missing, f"Schema entries without SQL files: {missing}"


# ── Custom tests ──────────────────────────────────────────────────────────────


class TestCustomTests:
    def test_negative_trade_value_test_exists(self):
        assert (TESTS_DIR / "assert_no_negative_trade_value.sql").exists()

    def test_negative_trade_value_test_references_stg_preview(self):
        sql = (TESTS_DIR / "assert_no_negative_trade_value.sql").read_text()
        assert "ref('stg_preview')" in sql

    def test_negative_trade_value_test_filters_negative_values(self):
        sql = (TESTS_DIR / "assert_no_negative_trade_value.sql").read_text()
        assert "< 0" in sql
