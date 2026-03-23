"""
Unit tests for plugins/comtrade/iceberg.py

pyiceberg and pyarrow are NOT installed in the test environment.
All external packages are injected via sys.modules so the module can be
imported and tested without them.
"""
from __future__ import annotations

import sys
from types import ModuleType
from unittest import mock

import pytest


# ── sys.modules injection helpers ─────────────────────────────────────────────


def _make_pa_table(rows: list) -> mock.MagicMock:
    """Return a MagicMock that behaves like a pyarrow Table."""
    t = mock.MagicMock()
    t.__len__ = mock.Mock(return_value=len(rows))
    t.schema = mock.MagicMock()
    return t


def _fake_pyarrow_modules(rows: list | None = None):
    """Inject fake pyarrow into sys.modules and return context manager."""
    rows = rows or [{"a": 1}]
    pa_table = _make_pa_table(rows)

    pa = ModuleType("pyarrow")
    pa.Table = mock.MagicMock()
    pa.Table.from_pylist = mock.Mock(return_value=pa_table)

    return mock.patch.dict(sys.modules, {"pyarrow": pa}), pa_table


def _fake_pyiceberg_modules(table=None):
    """Inject fake pyiceberg into sys.modules and return context manager."""
    table = table or mock.MagicMock()

    # pyiceberg.catalog
    catalog_mod = ModuleType("pyiceberg.catalog")
    catalog_mod.load_catalog = mock.Mock(return_value=mock.MagicMock())

    # pyiceberg.exceptions
    exceptions_mod = ModuleType("pyiceberg.exceptions")

    class NoSuchTableError(Exception):
        pass

    exceptions_mod.NoSuchTableError = NoSuchTableError

    # pyiceberg top-level
    pyiceberg_mod = ModuleType("pyiceberg")

    return (
        mock.patch.dict(
            sys.modules,
            {
                "pyiceberg": pyiceberg_mod,
                "pyiceberg.catalog": catalog_mod,
                "pyiceberg.exceptions": exceptions_mod,
            },
        ),
        catalog_mod,
        exceptions_mod,
    )


# ── Pure helper tests ──────────────────────────────────────────────────────────


class TestPureHelpers:
    def test_warehouse_uri(self):
        from comtrade.iceberg import _warehouse_uri

        assert _warehouse_uri("my-bucket") == "s3://my-bucket/iceberg"

    def test_table_identifier(self):
        from comtrade.iceberg import _table_identifier

        assert _table_identifier("preview") == "comtrade.preview"
        assert _table_identifier("getMBS") == "comtrade.getMBS"

    def test_table_location(self):
        from comtrade.iceberg import _table_location

        assert _table_location("bucket", "preview") == "s3://bucket/iceberg/preview"


# ── _add_loaded_at ────────────────────────────────────────────────────────────


class TestAddLoadedAt:
    def test_injects_loaded_at_key(self):
        from comtrade.iceberg import _add_loaded_at

        result = _add_loaded_at([{"a": 1}])
        assert "_loaded_at" in result[0]

    def test_preserves_original_fields(self):
        from comtrade.iceberg import _add_loaded_at

        result = _add_loaded_at([{"reportercode": "840", "primaryvalue": "100"}])
        assert result[0]["reportercode"] == "840"
        assert result[0]["primaryvalue"] == "100"

    def test_loaded_at_is_iso8601_utc(self):
        from datetime import datetime, timezone
        from comtrade.iceberg import _add_loaded_at

        result = _add_loaded_at([{"x": 1}])
        ts = result[0]["_loaded_at"]
        # Must parse as a valid datetime with UTC offset
        parsed = datetime.fromisoformat(ts)
        assert parsed.tzinfo is not None

    def test_all_records_share_same_timestamp(self):
        from comtrade.iceberg import _add_loaded_at

        records = [{"a": 1}, {"b": 2}, {"c": 3}]
        result = _add_loaded_at(records)
        timestamps = {r["_loaded_at"] for r in result}
        assert len(timestamps) == 1, "All records in a batch must share the same _loaded_at"

    def test_empty_list_returns_empty(self):
        from comtrade.iceberg import _add_loaded_at

        assert _add_loaded_at([]) == []

    def test_does_not_mutate_original_records(self):
        from comtrade.iceberg import _add_loaded_at

        original = [{"a": 1}]
        _add_loaded_at(original)
        assert "_loaded_at" not in original[0]

    def test_loaded_at_injected_before_pa_table_creation(self):
        """write_to_iceberg must call _records_to_pa_table with stamped records."""
        import sys
        from types import ModuleType
        from unittest import mock

        rows = [{"reportercode": "840"}]
        pa_patch, pa_table = _fake_pyarrow_modules(rows)
        ic_patch, catalog_mod, exceptions_mod = _fake_pyiceberg_modules()

        captured: list = []

        with pa_patch, ic_patch:
            import pyarrow as pa
            original_from_pylist = pa.Table.from_pylist

            def _capturing_from_pylist(records):
                captured.extend(records)
                return pa_table

            pa.Table.from_pylist = mock.Mock(side_effect=_capturing_from_pylist)

            from comtrade.iceberg import write_to_iceberg
            write_to_iceberg(rows, endpoint="preview", bucket="b", region="us-east-1")

        assert len(captured) == 1
        assert "_loaded_at" in captured[0]


# ── _records_to_pa_table ──────────────────────────────────────────────────────


class TestRecordsToPaTable:
    def test_converts_records(self):
        rows = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
        pa_patch, pa_table = _fake_pyarrow_modules(rows)
        with pa_patch:
            from comtrade.iceberg import _records_to_pa_table

            result = _records_to_pa_table(rows)
        assert result is pa_table

    def test_calls_from_pylist(self):
        rows = [{"x": 10}]
        pa_patch, pa_table = _fake_pyarrow_modules(rows)
        with pa_patch:
            import pyarrow as pa
            from comtrade.iceberg import _records_to_pa_table

            _records_to_pa_table(rows)
            pa.Table.from_pylist.assert_called_once_with(rows)


# ── _get_catalog ──────────────────────────────────────────────────────────────


class TestGetCatalog:
    def test_loads_glue_catalog(self):
        _, catalog_mod, _ = _fake_pyiceberg_modules()
        ic_patch = mock.patch.dict(
            sys.modules,
            {
                "pyiceberg": ModuleType("pyiceberg"),
                "pyiceberg.catalog": catalog_mod,
                "pyiceberg.exceptions": ModuleType("pyiceberg.exceptions"),
            },
        )
        with ic_patch:
            from comtrade.iceberg import _get_catalog

            _get_catalog(region="us-east-1", warehouse="s3://bucket/iceberg")
            catalog_mod.load_catalog.assert_called_once_with(
                "glue",
                **{
                    "type": "glue",
                    "region_name": "us-east-1",
                    "warehouse": "s3://bucket/iceberg",
                },
            )


# ── _ensure_namespace ─────────────────────────────────────────────────────────


class TestEnsureNamespace:
    def test_creates_namespace_if_missing(self):
        from comtrade.iceberg import _ensure_namespace

        catalog = mock.MagicMock()
        catalog.list_namespaces.return_value = [("other_db",)]
        _ensure_namespace(catalog, "comtrade")
        catalog.create_namespace.assert_called_once_with("comtrade")

    def test_no_create_if_namespace_exists(self):
        from comtrade.iceberg import _ensure_namespace

        catalog = mock.MagicMock()
        catalog.list_namespaces.return_value = [("comtrade",)]
        _ensure_namespace(catalog, "comtrade")
        catalog.create_namespace.assert_not_called()

    def test_logs_warning_on_error(self, caplog):
        import logging

        from comtrade.iceberg import _ensure_namespace

        catalog = mock.MagicMock()
        catalog.list_namespaces.side_effect = RuntimeError("boom")
        with caplog.at_level(logging.WARNING, logger="comtrade.iceberg"):
            _ensure_namespace(catalog, "comtrade")
        assert "Could not create namespace" in caplog.text


# ── _load_or_create_table ─────────────────────────────────────────────────────


class TestLoadOrCreateTable:
    def _setup(self, raise_no_such_table: bool = False):
        """Return (ic_patch, catalog, pa_table, NoSuchTableError)."""
        _, catalog_mod, exceptions_mod = _fake_pyiceberg_modules()

        table = mock.MagicMock()
        pa_table = _make_pa_table([{"a": 1}])

        mock_catalog = mock.MagicMock()
        if raise_no_such_table:
            mock_catalog.load_table.side_effect = exceptions_mod.NoSuchTableError
            mock_catalog.create_table.return_value = table
        else:
            mock_catalog.load_table.return_value = table

        ic_patch = mock.patch.dict(
            sys.modules,
            {
                "pyiceberg": ModuleType("pyiceberg"),
                "pyiceberg.catalog": catalog_mod,
                "pyiceberg.exceptions": exceptions_mod,
            },
        )
        return ic_patch, mock_catalog, pa_table, table, exceptions_mod.NoSuchTableError

    def test_returns_existing_table(self):
        ic_patch, catalog, pa_table, table, _ = self._setup(raise_no_such_table=False)
        with ic_patch:
            from comtrade.iceberg import _load_or_create_table

            result = _load_or_create_table(catalog, "comtrade.preview", pa_table, "s3://b/iceberg/preview")
        assert result is table

    def test_evolves_schema_on_existing_table(self):
        ic_patch, catalog, pa_table, table, _ = self._setup(raise_no_such_table=False)
        with ic_patch:
            from comtrade.iceberg import _load_or_create_table

            _load_or_create_table(catalog, "comtrade.preview", pa_table, "s3://b/iceberg/preview")
            # update_schema context manager should have been entered
            table.update_schema.assert_called_once()

    def test_creates_table_when_not_found(self):
        ic_patch, catalog, pa_table, table, _ = self._setup(raise_no_such_table=True)
        # Also need list_namespaces for _ensure_namespace
        catalog.list_namespaces.return_value = [("comtrade",)]
        with ic_patch:
            from comtrade.iceberg import _load_or_create_table

            result = _load_or_create_table(
                catalog, "comtrade.preview", pa_table, "s3://b/iceberg/preview"
            )
        catalog.create_table.assert_called_once_with(
            identifier="comtrade.preview",
            schema=pa_table.schema,
            location="s3://b/iceberg/preview",
        )
        assert result is table


# ── write_to_iceberg ──────────────────────────────────────────────────────────


class TestWriteToIceberg:
    def _patches(self, rows=None):
        """Return context managers for pa + pyiceberg mocks."""
        rows = rows or [{"a": 1}]
        pa_patch, pa_table = _fake_pyarrow_modules(rows)
        _, catalog_mod, exceptions_mod = _fake_pyiceberg_modules()

        mock_table = mock.MagicMock()
        snapshot = mock.MagicMock()
        snapshot.snapshot_id = 42
        mock_table.current_snapshot.return_value = snapshot

        mock_catalog = mock.MagicMock()
        mock_catalog.load_table.return_value = mock_table
        catalog_mod.load_catalog.return_value = mock_catalog

        ic_patch = mock.patch.dict(
            sys.modules,
            {
                "pyiceberg": ModuleType("pyiceberg"),
                "pyiceberg.catalog": catalog_mod,
                "pyiceberg.exceptions": exceptions_mod,
            },
        )
        return pa_patch, ic_patch, pa_table, mock_table

    def test_returns_identifier_on_success(self):
        pa_patch, ic_patch, _, _ = self._patches()
        with pa_patch, ic_patch:
            from comtrade.iceberg import write_to_iceberg

            result = write_to_iceberg(
                records=[{"a": 1}], endpoint="preview", bucket="my-bucket"
            )
        assert result == "comtrade.preview"

    def test_appends_pa_table(self):
        pa_patch, ic_patch, pa_table, mock_table = self._patches()
        with pa_patch, ic_patch:
            from comtrade.iceberg import write_to_iceberg

            write_to_iceberg(records=[{"a": 1}], endpoint="preview", bucket="my-bucket")
            mock_table.append.assert_called_once_with(pa_table)

    def test_returns_none_for_empty_records(self):
        pa_patch, ic_patch, _, _ = self._patches([])
        with pa_patch, ic_patch:
            from comtrade.iceberg import write_to_iceberg

            result = write_to_iceberg(records=[], endpoint="preview", bucket="my-bucket")
        assert result is None

    def test_returns_none_on_exception(self, caplog):
        import logging

        pa_patch, ic_patch, _, _ = self._patches()
        with pa_patch, ic_patch:
            import pyarrow as pa

            pa.Table.from_pylist.side_effect = RuntimeError("pyarrow error")
            from comtrade.iceberg import write_to_iceberg

            with caplog.at_level(logging.ERROR, logger="comtrade.iceberg"):
                result = write_to_iceberg(
                    records=[{"a": 1}], endpoint="preview", bucket="my-bucket"
                )
        assert result is None
        assert "Failed to write Iceberg table" in caplog.text

    def test_uses_provided_region(self):
        pa_patch, ic_patch, _, _ = self._patches()
        with pa_patch, ic_patch:
            import pyiceberg.catalog as cat

            from comtrade.iceberg import write_to_iceberg

            write_to_iceberg(
                records=[{"a": 1}],
                endpoint="preview",
                bucket="my-bucket",
                region="eu-west-1",
            )
            call_kwargs = cat.load_catalog.call_args[1]
            assert call_kwargs["region_name"] == "eu-west-1"

    def test_snapshot_id_logged(self, caplog):
        import logging

        pa_patch, ic_patch, _, _ = self._patches()
        with pa_patch, ic_patch:
            from comtrade.iceberg import write_to_iceberg

            with caplog.at_level(logging.INFO, logger="comtrade.iceberg"):
                write_to_iceberg(
                    records=[{"a": 1}], endpoint="preview", bucket="my-bucket"
                )
        assert "snapshot=42" in caplog.text
