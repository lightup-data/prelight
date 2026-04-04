"""Tests for sandbox_manager: registry, creation, lookup, and logging."""

from __future__ import annotations

from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from prelight.core.sandbox_manager import (
    SandboxRecord,
    _registry,
    create_sandbox,
    get_sandbox,
    get_sandbox_for_table,
    list_sandboxes,
    log_transformation,
    mark_quality_passed,
    store_quality_results,
)


@pytest.fixture(autouse=True)
def _clear_registry():
    """Ensure registry is empty before and after each test."""
    _registry.clear()
    yield
    _registry.clear()


# ── SandboxRecord ───────────────────────────────────────────────────────────


class TestSandboxRecord:
    def test_defaults(self):
        r = SandboxRecord(sandbox_name="sbx_t_1234", source_table="t", created_at="2026-01-01T00:00:00")
        assert r.applied_sqls == []
        assert r.quality_passed is False
        assert r.quality_run_id is None
        assert r.quality_check_results == []
        assert r.custom_quality_checks == []
        assert r.schema_columns == []
        assert r.migration_file_path is None


# ── create_sandbox ──────────────────────────────────────────────────────────


class TestCreateSandbox:
    @patch("prelight.core.sandbox_manager.get_client")
    @patch("prelight.core.sandbox_manager.get_settings")
    def test_creates_and_registers(self, mock_settings, mock_client):
        mock_settings.return_value = MagicMock(
            db_schema="analytics",
            sandbox_prefix="sbx_",
        )
        client = MagicMock()
        client.get_table_schema.return_value = [
            {"column_name": "id", "data_type": "BIGINT", "nullable": False}
        ]
        mock_client.return_value = client

        record = create_sandbox("orders")

        assert record.sandbox_name.startswith("sbx_orders_")
        assert record.source_table == "orders"
        assert record.schema_columns == [
            {"column_name": "id", "data_type": "BIGINT", "nullable": False}
        ]
        client.create_sandbox_table.assert_called_once()
        assert record.sandbox_name in _registry

    @patch("prelight.core.sandbox_manager.get_client")
    @patch("prelight.core.sandbox_manager.get_settings")
    def test_multiple_sandboxes_for_same_table(self, mock_settings, mock_client):
        """Two sandboxes for the same table should both be registered.

        Note: sandbox names include a minute-precision timestamp, so two calls
        within the same minute may produce the same name. We mock datetime to
        ensure distinct timestamps.
        """
        mock_settings.return_value = MagicMock(db_schema="analytics", sandbox_prefix="sbx_")
        client = MagicMock()
        client.get_table_schema.return_value = []
        mock_client.return_value = client

        from datetime import datetime, timezone
        t1 = datetime(2026, 4, 3, 10, 0, tzinfo=timezone.utc)
        t2 = datetime(2026, 4, 3, 10, 1, tzinfo=timezone.utc)

        with patch("prelight.core.sandbox_manager.datetime") as mock_dt:
            mock_dt.now.return_value = t1
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            r1 = create_sandbox("orders")

            mock_dt.now.return_value = t2
            r2 = create_sandbox("orders")

        assert r1.sandbox_name != r2.sandbox_name
        assert len(_registry) == 2


# ── get_sandbox ─────────────────────────────────────────────────────────────


class TestGetSandbox:
    def test_found(self):
        r = SandboxRecord(sandbox_name="sbx_test", source_table="test", created_at="2026-01-01")
        _registry["sbx_test"] = r
        assert get_sandbox("sbx_test") is r

    def test_not_found(self):
        with pytest.raises(ValueError, match="No sandbox found"):
            get_sandbox("nonexistent")


# ── get_sandbox_for_table ───────────────────────────────────────────────────


class TestGetSandboxForTable:
    def test_returns_none_when_empty(self):
        assert get_sandbox_for_table("orders") is None

    def test_returns_latest(self):
        r1 = SandboxRecord(sandbox_name="sbx_orders_1", source_table="orders", created_at="2026-01-01")
        r2 = SandboxRecord(sandbox_name="sbx_orders_2", source_table="orders", created_at="2026-01-02")
        _registry["sbx_orders_1"] = r1
        _registry["sbx_orders_2"] = r2
        assert get_sandbox_for_table("orders") is r2

    def test_ignores_other_tables(self):
        r1 = SandboxRecord(sandbox_name="sbx_customers_1", source_table="customers", created_at="2026-01-02")
        _registry["sbx_customers_1"] = r1
        assert get_sandbox_for_table("orders") is None


# ── log_transformation ──────────────────────────────────────────────────────


class TestLogTransformation:
    def test_appends_sql(self):
        r = SandboxRecord(sandbox_name="sbx_t", source_table="t", created_at="2026-01-01")
        _registry["sbx_t"] = r
        log_transformation("sbx_t", "UPDATE sbx_t SET x = 1")
        log_transformation("sbx_t", "ALTER TABLE sbx_t ADD COLUMN y INT")
        assert len(r.applied_sqls) == 2

    def test_raises_for_unknown_sandbox(self):
        with pytest.raises(ValueError):
            log_transformation("nonexistent", "SELECT 1")


# ── store_quality_results & mark_quality_passed ─────────────────────────────


class TestQualityTracking:
    def test_store_results(self):
        r = SandboxRecord(sandbox_name="sbx_t", source_table="t", created_at="2026-01-01")
        _registry["sbx_t"] = r
        checks = [{"check": "row_count", "status": "PASS"}]
        store_quality_results("sbx_t", "run-123", checks)
        assert r.quality_run_id == "run-123"
        assert r.quality_check_results == checks

    def test_mark_passed(self):
        r = SandboxRecord(sandbox_name="sbx_t", source_table="t", created_at="2026-01-01")
        _registry["sbx_t"] = r
        assert r.quality_passed is False
        mark_quality_passed("sbx_t")
        assert r.quality_passed is True


# ── list_sandboxes ──────────────────────────────────────────────────────────


class TestListSandboxes:
    def test_empty(self):
        assert list_sandboxes() == []

    def test_returns_all(self):
        _registry["a"] = SandboxRecord(sandbox_name="a", source_table="t1", created_at="2026-01-01")
        _registry["b"] = SandboxRecord(sandbox_name="b", source_table="t2", created_at="2026-01-02")
        assert len(list_sandboxes()) == 2
