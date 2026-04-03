"""Tests for sql_utils: identifier validation, slug generation, and SQL rewriting."""

from __future__ import annotations

import pytest

from prelight.core.sql_utils import rewrite_to_production, slug, validate_identifier


# ── validate_identifier ─────────────────────────────────────────────────────


class TestValidateIdentifier:
    """Identifier validation must block injection attempts and allow normal names."""

    # ── Should pass ──

    def test_simple_name(self):
        assert validate_identifier("orders") == "orders"

    def test_with_underscore(self):
        assert validate_identifier("sbx_orders_20260403") == "sbx_orders_20260403"

    def test_schema_dot_table(self):
        assert validate_identifier("analytics.orders") == "analytics.orders"

    def test_leading_underscore(self):
        assert validate_identifier("_internal") == "_internal"

    def test_mixed_case(self):
        assert validate_identifier("MyTable") == "MyTable"

    def test_strips_whitespace(self):
        assert validate_identifier("  orders  ") == "orders"

    # ── Should reject ──

    def test_empty_string(self):
        with pytest.raises(ValueError, match="must not be empty"):
            validate_identifier("")

    def test_whitespace_only(self):
        with pytest.raises(ValueError, match="must not be empty"):
            validate_identifier("   ")

    def test_semicolon_injection(self):
        with pytest.raises(ValueError, match="Invalid"):
            validate_identifier("orders; DROP TABLE users--")

    def test_single_quote_injection(self):
        with pytest.raises(ValueError, match="Invalid"):
            validate_identifier("orders' OR '1'='1")

    def test_double_quote(self):
        with pytest.raises(ValueError, match="Invalid"):
            validate_identifier('"orders"')

    def test_parentheses(self):
        with pytest.raises(ValueError, match="Invalid"):
            validate_identifier("orders()")

    def test_spaces_in_name(self):
        with pytest.raises(ValueError, match="Invalid"):
            validate_identifier("my table")

    def test_backtick(self):
        with pytest.raises(ValueError, match="Invalid"):
            validate_identifier("`orders`")

    def test_dash_in_name(self):
        with pytest.raises(ValueError, match="Invalid"):
            validate_identifier("my-table")

    def test_leading_digit(self):
        with pytest.raises(ValueError, match="Invalid"):
            validate_identifier("123table")

    def test_newline_injection(self):
        with pytest.raises(ValueError, match="Invalid"):
            validate_identifier("orders\n; DROP TABLE users")

    def test_null_byte(self):
        with pytest.raises(ValueError, match="Invalid"):
            validate_identifier("orders\x00")

    def test_custom_label(self):
        with pytest.raises(ValueError, match="schema"):
            validate_identifier("bad;name", label="schema")


# ── slug ────────────────────────────────────────────────────────────────────


class TestSlug:
    def test_basic(self):
        assert slug("Apply 10% discount to orders") == "apply-10-discount-to-orders"

    def test_strips_special_chars(self):
        assert slug("hello!@#world") == "hello-world"

    def test_max_length(self):
        result = slug("a very long description that goes on and on", max_len=10)
        assert len(result) <= 10

    def test_leading_trailing_hyphens_stripped(self):
        assert slug("--hello--") == "hello"

    def test_empty_string(self):
        assert slug("") == ""

    def test_only_special_chars(self):
        assert slug("!@#$%") == ""


# ── rewrite_to_production ───────────────────────────────────────────────────


class TestRewriteToProduction:
    def test_basic_rewrite(self):
        sql = "UPDATE analytics.sbx_orders_20260403 SET status = 'done'"
        result = rewrite_to_production(sql, "analytics", "sbx_orders_20260403", "orders")
        assert "analytics.orders" in result
        assert "sbx_orders_20260403" not in result

    def test_unqualified_rewrite(self):
        sql = "UPDATE sbx_orders_20260403 SET status = 'done'"
        result = rewrite_to_production(sql, "analytics", "sbx_orders_20260403", "orders")
        assert "orders" in result
        assert "sbx_orders_20260403" not in result

    def test_case_insensitive(self):
        sql = "UPDATE analytics.SBX_ORDERS_20260403 SET status = 'done'"
        result = rewrite_to_production(sql, "analytics", "sbx_orders_20260403", "orders")
        assert "analytics.orders" in result

    def test_multiple_occurrences(self):
        sql = (
            "UPDATE analytics.sbx_orders_20260403 SET total = "
            "(SELECT SUM(amount) FROM analytics.sbx_orders_20260403)"
        )
        result = rewrite_to_production(sql, "analytics", "sbx_orders_20260403", "orders")
        assert result.count("sbx_orders_20260403") == 0
        assert result.count("analytics.orders") == 2

    def test_preserves_other_text(self):
        sql = "ALTER TABLE sbx_orders_20260403 ADD COLUMN region VARCHAR"
        result = rewrite_to_production(sql, "analytics", "sbx_orders_20260403", "orders")
        assert "ALTER TABLE" in result
        assert "ADD COLUMN region VARCHAR" in result
