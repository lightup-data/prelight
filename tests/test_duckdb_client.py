"""Integration tests for duckdb_client with a real in-memory DuckDB instance.

These tests validate that input sanitization works end-to-end and that
basic CRUD operations function correctly against a real database.
"""

from __future__ import annotations

import pytest

from prelight.core.clients import duckdb_client
from prelight.core.clients.duckdb_client import (
    create_sandbox_table,
    execute_query,
    execute_statement,
    get_row_count,
    get_table_schema,
    list_table_names,
    reset_connection,
    table_exists,
)


@pytest.fixture(autouse=True)
def _fresh_connection(duckdb_config_with_path):
    """Ensure each test gets a fresh DuckDB connection with a real file."""
    reset_connection()
    # Create the analytics schema and a test table
    execute_statement("CREATE SCHEMA IF NOT EXISTS analytics")
    execute_statement(
        "CREATE TABLE IF NOT EXISTS analytics.test_table ("
        "  id BIGINT, name VARCHAR, amount DOUBLE"
        ")"
    )
    execute_statement(
        "INSERT INTO analytics.test_table VALUES "
        "(1, 'Alice', 100.0), (2, 'Bob', 200.0), (3, 'Carol', 300.0)"
    )
    yield
    reset_connection()


# ── Basic operations ────────────────────────────────────────────────────────


class TestBasicOperations:
    def test_execute_query(self):
        rows = execute_query("SELECT * FROM analytics.test_table ORDER BY id")
        assert len(rows) == 3
        assert rows[0]["id"] == 1
        assert rows[0]["name"] == "Alice"

    def test_execute_statement(self):
        execute_statement(
            "CREATE TABLE analytics.sbx_test AS SELECT * FROM analytics.test_table"
        )
        rows = execute_query("SELECT COUNT(*) AS cnt FROM analytics.sbx_test")
        assert rows[0]["cnt"] == 3

    def test_get_table_schema(self):
        schema = get_table_schema("test_table")
        col_names = [c["column_name"] for c in schema]
        assert "id" in col_names
        assert "name" in col_names
        assert "amount" in col_names

    def test_get_row_count(self):
        assert get_row_count("test_table") == 3

    def test_list_table_names(self):
        tables = list_table_names("analytics")
        assert "test_table" in tables

    def test_table_exists_true(self):
        assert table_exists("test_table") is True

    def test_table_exists_false(self):
        assert table_exists("nonexistent_table") is False

    def test_create_sandbox_table(self):
        create_sandbox_table("analytics.test_table", "analytics.sbx_test_copy")
        assert table_exists("sbx_test_copy") is True
        assert get_row_count("sbx_test_copy") == 3


# ── Input sanitization ──────────────────────────────────────────────────────


class TestInputSanitization:
    """Verify that injection attempts via table/schema names are caught."""

    def test_get_table_schema_injection(self):
        with pytest.raises(ValueError, match="Invalid"):
            get_table_schema("test_table; DROP TABLE analytics.test_table--")

    def test_get_row_count_injection(self):
        with pytest.raises(ValueError, match="Invalid"):
            get_row_count("test_table; DROP TABLE analytics.test_table--")

    def test_list_table_names_injection(self):
        with pytest.raises(ValueError, match="Invalid"):
            list_table_names("analytics'; DROP SCHEMA analytics CASCADE--")

    def test_create_sandbox_injection_source(self):
        with pytest.raises(ValueError, match="Invalid"):
            create_sandbox_table(
                "analytics.test_table; DROP TABLE analytics.test_table",
                "analytics.sbx_safe",
            )

    def test_create_sandbox_injection_target(self):
        with pytest.raises(ValueError, match="Invalid"):
            create_sandbox_table(
                "analytics.test_table",
                "analytics.sbx_safe; DROP TABLE analytics.test_table",
            )

    def test_table_exists_injection(self):
        with pytest.raises(ValueError, match="Invalid"):
            table_exists("test' OR '1'='1")
