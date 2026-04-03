"""Tests for the production guard — the core safety mechanism.

The guard MUST:
  - Block writes (INSERT/UPDATE/DELETE/DROP/ALTER/TRUNCATE/MERGE) to production tables
  - Allow writes to sandbox-prefixed tables
  - Allow writes to the audit table
  - Allow SELECT queries unconditionally
  - FAIL CLOSED: reject unparseable SQL rather than allowing it through
"""

from __future__ import annotations

import pytest

from prelight.core.production_guard import (
    ProductionWriteBlockedError,
    check_select_only,
    check_sql,
)

PREFIX = "sbx_"
AUDIT = "qg_quality_results"


# ── check_sql: should BLOCK writes to production ────────────────────────────


class TestCheckSqlBlocksProduction:
    """Every write operation type targeting a production table must be blocked."""

    def test_insert_into_production(self):
        with pytest.raises(ProductionWriteBlockedError, match="INSERT"):
            check_sql("INSERT INTO analytics.orders VALUES (1, 2, 3)", PREFIX, AUDIT)

    def test_update_production(self):
        with pytest.raises(ProductionWriteBlockedError, match="UPDATE"):
            check_sql("UPDATE orders SET status = 'archived' WHERE id = 1", PREFIX, AUDIT)

    def test_delete_from_production(self):
        with pytest.raises(ProductionWriteBlockedError, match="DELETE"):
            check_sql("DELETE FROM orders WHERE id = 1", PREFIX, AUDIT)

    def test_drop_production(self):
        with pytest.raises(ProductionWriteBlockedError, match="DROP"):
            check_sql("DROP TABLE orders", PREFIX, AUDIT)

    def test_alter_production(self):
        with pytest.raises(ProductionWriteBlockedError, match="ALTER"):
            check_sql("ALTER TABLE orders ADD COLUMN region VARCHAR", PREFIX, AUDIT)

    def test_truncate_production(self):
        with pytest.raises(ProductionWriteBlockedError, match="TRUNCATE"):
            check_sql("TRUNCATE TABLE orders", PREFIX, AUDIT)

    def test_merge_into_production(self):
        sql = (
            "MERGE INTO orders AS target "
            "USING new_orders AS source ON target.id = source.id "
            "WHEN MATCHED THEN UPDATE SET status = source.status"
        )
        with pytest.raises(ProductionWriteBlockedError, match="MERGE"):
            check_sql(sql, PREFIX, AUDIT)

    def test_create_or_replace_production(self):
        with pytest.raises(ProductionWriteBlockedError, match="CREATE OR REPLACE"):
            check_sql("CREATE OR REPLACE TABLE orders AS SELECT 1", PREFIX, AUDIT)

    def test_blocks_schema_qualified_production(self):
        with pytest.raises(ProductionWriteBlockedError):
            check_sql("UPDATE analytics.orders SET x = 1", PREFIX, AUDIT)

    def test_blocks_quoted_production_table(self):
        """Quoted identifiers should still be caught."""
        with pytest.raises(ProductionWriteBlockedError):
            check_sql('UPDATE "orders" SET x = 1', PREFIX, AUDIT)


# ── check_sql: should ALLOW writes to sandbox / audit ───────────────────────


class TestCheckSqlAllowsSandbox:
    """Writes to sandbox-prefixed tables must pass."""

    def test_update_sandbox(self):
        check_sql("UPDATE sbx_orders_20260403 SET status = 'archived'", PREFIX, AUDIT)

    def test_insert_sandbox(self):
        check_sql("INSERT INTO analytics.sbx_orders_20260403 VALUES (1, 2)", PREFIX, AUDIT)

    def test_delete_sandbox(self):
        check_sql("DELETE FROM sbx_orders_20260403 WHERE id = 1", PREFIX, AUDIT)

    def test_alter_sandbox(self):
        check_sql("ALTER TABLE sbx_orders_20260403 ADD COLUMN region VARCHAR", PREFIX, AUDIT)

    def test_drop_sandbox(self):
        check_sql("DROP TABLE sbx_orders_20260403", PREFIX, AUDIT)

    def test_create_or_replace_sandbox(self):
        check_sql("CREATE OR REPLACE TABLE sbx_orders_20260403 AS SELECT 1", PREFIX, AUDIT)


class TestCheckSqlAllowsAudit:
    """Writes to the audit table must pass."""

    def test_insert_audit(self):
        check_sql("INSERT INTO qg_quality_results VALUES (1, 'pass')", PREFIX, AUDIT)

    def test_update_audit(self):
        check_sql("UPDATE qg_quality_results SET status = 'done'", PREFIX, AUDIT)


class TestCheckSqlAllowsSelect:
    """SELECT (read-only) should always pass."""

    def test_simple_select(self):
        check_sql("SELECT * FROM orders", PREFIX, AUDIT)

    def test_select_with_where(self):
        check_sql("SELECT * FROM orders WHERE status = 'completed'", PREFIX, AUDIT)

    def test_select_with_join(self):
        check_sql(
            "SELECT o.*, c.name FROM orders o JOIN customers c ON o.customer_id = c.customer_id",
            PREFIX, AUDIT,
        )

    def test_with_cte_select(self):
        check_sql(
            "WITH recent AS (SELECT * FROM orders WHERE created_at > '2024-01-01') "
            "SELECT * FROM recent",
            PREFIX, AUDIT,
        )

    def test_create_table_without_replace_allowed(self):
        """Plain CREATE TABLE (not OR REPLACE) for sandbox."""
        check_sql("CREATE TABLE sbx_test AS SELECT 1", PREFIX, AUDIT)


# ── check_sql: FAIL CLOSED on unparseable SQL ──────────────────────────────


class TestCheckSqlFailsClosed:
    """When sqlglot raises a parse exception, it MUST be rejected, never allowed through.

    Note: sqlglot is surprisingly lenient with WARN error level — it can parse many
    nonsensical strings as valid SQL expressions (e.g., 'THIS IS NOT SQL' becomes a
    NOT/IS expression). The fail-closed behaviour applies when sqlglot actually raises
    an exception, not when it manages to produce an AST.
    """

    def test_parse_exception_blocks_check_sql(self):
        """If sqlglot.parse raises, check_sql must raise ProductionWriteBlockedError."""
        import sqlglot as _sqlglot
        from unittest.mock import patch

        with patch.object(_sqlglot, "parse", side_effect=Exception("deliberate parse failure")):
            with pytest.raises(ProductionWriteBlockedError, match="could not be parsed"):
                check_sql("anything", PREFIX, AUDIT)

    def test_parse_exception_blocks_check_select_only(self):
        """If sqlglot.parse raises, check_select_only must raise ProductionWriteBlockedError."""
        import sqlglot as _sqlglot
        from unittest.mock import patch

        with patch.object(_sqlglot, "parse", side_effect=Exception("deliberate parse failure")):
            with pytest.raises(ProductionWriteBlockedError, match="could not be parsed"):
                check_select_only("anything")

    def test_non_select_garbage_blocked_by_check_select_only(self):
        """Even if sqlglot parses garbage without error, check_select_only should
        still block it because the result won't be a SELECT statement."""
        # sqlglot parses 'GARBLE THINGS' as an expression, not a SELECT
        with pytest.raises(ProductionWriteBlockedError, match="Only SELECT"):
            check_select_only("GARBLE THINGS")


# ── check_select_only ───────────────────────────────────────────────────────


class TestCheckSelectOnly:
    """The stricter check for query_table — only SELECT allowed."""

    def test_select_passes(self):
        check_select_only("SELECT * FROM orders")

    def test_with_select_passes(self):
        check_select_only(
            "WITH x AS (SELECT 1 AS n) SELECT * FROM x"
        )

    def test_insert_blocked(self):
        with pytest.raises(ProductionWriteBlockedError, match="Only SELECT"):
            check_select_only("INSERT INTO orders VALUES (1)")

    def test_update_blocked(self):
        with pytest.raises(ProductionWriteBlockedError, match="Only SELECT"):
            check_select_only("UPDATE orders SET x = 1")

    def test_delete_blocked(self):
        with pytest.raises(ProductionWriteBlockedError, match="Only SELECT"):
            check_select_only("DELETE FROM orders")

    def test_drop_blocked(self):
        with pytest.raises(ProductionWriteBlockedError, match="Only SELECT"):
            check_select_only("DROP TABLE orders")

    def test_non_select_statement_blocked(self):
        """Any statement that isn't SELECT should be blocked, even if it parses."""
        with pytest.raises(ProductionWriteBlockedError, match="Only SELECT"):
            check_select_only("CREATE TABLE foo (id INT)")


# ── Edge cases ──────────────────────────────────────────────────────────────


class TestEdgeCases:
    def test_empty_string(self):
        """Empty SQL should be a no-op, not an error."""
        check_sql("", PREFIX, AUDIT)

    def test_comment_only(self):
        """Comment-only SQL should pass."""
        check_sql("-- just a comment", PREFIX, AUDIT)

    def test_multiple_statements_one_bad(self):
        """If any statement in a batch targets production, block it."""
        sql = (
            "UPDATE sbx_orders SET x = 1; "
            "UPDATE orders SET x = 1"
        )
        with pytest.raises(ProductionWriteBlockedError):
            check_sql(sql, PREFIX, AUDIT)

    def test_case_insensitive_prefix(self):
        """Sandbox prefix matching should be case-insensitive."""
        check_sql("UPDATE SBX_orders_20260403 SET x = 1", PREFIX, AUDIT)

    def test_case_insensitive_audit(self):
        """Audit table matching should be case-insensitive."""
        check_sql("INSERT INTO QG_QUALITY_RESULTS VALUES (1)", PREFIX, AUDIT)
