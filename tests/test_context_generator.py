"""Tests for context_generator: column description inference and markdown generation."""

from __future__ import annotations

from prelight.core.context_generator import (
    _append_new_columns,
    _build_columns_table,
    _documented_column_names,
    _smart_column_desc,
    build_context_md,
)


# ── _smart_column_desc ──────────────────────────────────────────────────────


class TestSmartColumnDesc:
    def test_id_column(self):
        assert "identifier" in _smart_column_desc("id", "BIGINT").lower()

    def test_foreign_key(self):
        desc = _smart_column_desc("customer_id", "BIGINT")
        assert "customer" in desc.lower()

    def test_status_column(self):
        assert "status" in _smart_column_desc("status", "VARCHAR").lower()

    def test_amount_column(self):
        assert "monetary" in _smart_column_desc("amount", "DOUBLE").lower()

    def test_created_at(self):
        assert "created" in _smart_column_desc("created_at", "TIMESTAMP").lower()

    def test_updated_at(self):
        assert "update" in _smart_column_desc("updated_at", "TIMESTAMP").lower()

    def test_deleted_at(self):
        assert "soft-delete" in _smart_column_desc("deleted_at", "TIMESTAMP").lower()

    def test_event_timestamp(self):
        desc = _smart_column_desc("shipped_at", "TIMESTAMP")
        assert "shipped" in desc.lower()

    def test_boolean_flag(self):
        assert "boolean" in _smart_column_desc("is_active", "BOOLEAN").lower()

    def test_category_column(self):
        assert "categorical" in _smart_column_desc("order_type", "VARCHAR").lower()

    def test_percentage_column(self):
        assert "ratio" in _smart_column_desc("discount_pct", "DOUBLE").lower() or "percentage" in _smart_column_desc("discount_pct", "DOUBLE").lower()

    def test_count_column(self):
        assert "count" in _smart_column_desc("item_count", "INTEGER").lower()

    def test_name_column(self):
        assert "label" in _smart_column_desc("name", "VARCHAR").lower() or "name" in _smart_column_desc("name", "VARCHAR").lower()

    def test_email_column(self):
        assert "contact" in _smart_column_desc("email", "VARCHAR").lower()

    def test_geographic_column(self):
        assert "geographic" in _smart_column_desc("country", "VARCHAR").lower()

    def test_unknown_column_prompts(self):
        desc = _smart_column_desc("xyzzy_foo", "VARCHAR")
        assert "xyzzy_foo" in desc  # Should contain the column name as a prompt


# ── _build_columns_table ───────────────────────────────────────────────────


class TestBuildColumnsTable:
    def test_generates_markdown_table(self):
        cols = [
            {"column_name": "id", "data_type": "BIGINT"},
            {"column_name": "name", "data_type": "VARCHAR"},
        ]
        table = _build_columns_table(cols)
        assert "| Column | Type | Description |" in table
        assert "| id | BIGINT |" in table
        assert "| name | VARCHAR |" in table

    def test_empty_columns(self):
        table = _build_columns_table([])
        assert "Column" in table  # Header still present
        lines = table.strip().split("\n")
        assert len(lines) == 2  # Header + separator only


# ── _documented_column_names ────────────────────────────────────────────────


class TestDocumentedColumnNames:
    def test_parses_column_names(self):
        body = (
            "| Column | Type | Description |\n"
            "|--------|------|-------------|\n"
            "| id | BIGINT | Primary key |\n"
            "| name | VARCHAR | User name |\n"
        )
        names = _documented_column_names(body)
        assert names == {"id", "name"}

    def test_ignores_header_and_separator(self):
        body = (
            "| Column | Type | Description |\n"
            "|--------|------|-------------|\n"
        )
        names = _documented_column_names(body)
        assert len(names) == 0


# ── _append_new_columns ────────────────────────────────────────────────────


class TestAppendNewColumns:
    def test_adds_missing_column(self):
        existing_body = (
            "| Column | Type | Description |\n"
            "|--------|------|-------------|\n"
            "| id | BIGINT | Primary key |\n"
        )
        schema = [
            {"column_name": "id", "data_type": "BIGINT"},
            {"column_name": "region", "data_type": "VARCHAR"},
        ]
        result = _append_new_columns(existing_body, schema)
        assert "| region | VARCHAR |" in result
        # Existing row should still be there
        assert "| id | BIGINT |" in result

    def test_no_duplicates(self):
        existing_body = (
            "| Column | Type | Description |\n"
            "|--------|------|-------------|\n"
            "| id | BIGINT | Primary key |\n"
        )
        schema = [{"column_name": "id", "data_type": "BIGINT"}]
        result = _append_new_columns(existing_body, schema)
        assert result.count("| id |") == 1


# ── build_context_md ────────────────────────────────────────────────────────


class TestBuildContextMd:
    def test_fresh_generation(self):
        md = build_context_md(
            source_table="orders",
            schema="analytics",
            schema_columns=[{"column_name": "id", "data_type": "BIGINT"}],
            description="Add region column",
            iso_ts="2026-04-03T00:00:00",
        )
        assert "table: orders" in md
        assert "schema: analytics" in md
        assert "# orders" in md
        assert "## Purpose" in md
        assert "## Columns" in md
        assert "| id | BIGINT |" in md

    def test_fresh_with_context_notes(self):
        md = build_context_md(
            source_table="orders",
            schema="analytics",
            schema_columns=[],
            description="test",
            iso_ts="2026-04-03T00:00:00",
            context_notes="This table tracks all customer orders.",
        )
        assert "This table tracks all customer orders." in md
        assert "Auto-generated" not in md  # No warning banner when notes provided

    def test_update_preserves_existing_content(self):
        existing = (
            "---\n"
            "table: orders\n"
            "schema: analytics\n"
            "last_updated: 2026-01-01T00:00:00\n"
            "---\n"
            "\n"
            "# orders\n"
            "\n"
            "## Purpose\n"
            "\n"
            "This is my custom description.\n"
            "\n"
            "## Columns\n"
            "\n"
            "| Column | Type | Description |\n"
            "|--------|------|-------------|\n"
            "| id | BIGINT | My custom description |\n"
        )
        updated = build_context_md(
            source_table="orders",
            schema="analytics",
            schema_columns=[
                {"column_name": "id", "data_type": "BIGINT"},
                {"column_name": "region", "data_type": "VARCHAR"},
            ],
            description="test",
            iso_ts="2026-04-03T00:00:00",
            existing_content=existing,
        )
        # Custom description preserved
        assert "My custom description" in updated
        # Timestamp updated
        assert "2026-04-03T00:00:00" in updated
        assert "2026-01-01T00:00:00" not in updated
        # New column added
        assert "| region | VARCHAR |" in updated
