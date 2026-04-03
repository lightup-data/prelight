"""Tests for quality_checks: result evaluation and check execution."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from prelight.core.quality_checks import (
    _evaluate_result,
    _extract_failure_detail,
    _fmt_result_rows,
    run_custom_checks,
)


# ── _evaluate_result ────────────────────────────────────────────────────────


class TestEvaluateResult:
    """Test the pass/fail evaluation logic for quality check results."""

    def test_empty_rows_is_pass(self):
        """Zero rows returned = no violations = PASS."""
        status, msg = _evaluate_result([])
        assert status == "PASS"
        assert "no violations" in msg

    def test_status_column_all_pass(self):
        rows = [{"metric": "row_count", "status": "PASS"}]
        status, _ = _evaluate_result(rows)
        assert status == "PASS"

    def test_status_column_with_fail(self):
        rows = [
            {"metric": "row_count", "status": "PASS"},
            {"metric": "mean_drift", "status": "FAIL"},
        ]
        status, _ = _evaluate_result(rows)
        assert status == "FAIL"

    def test_status_column_case_insensitive(self):
        rows = [{"STATUS": "PASS"}]
        status, _ = _evaluate_result(rows)
        assert status == "PASS"

    def test_no_status_column_rows_present_is_fail(self):
        """Rows without a 'status' column are treated as violations."""
        rows = [{"orphaned_fk": 999, "count": 3}]
        status, _ = _evaluate_result(rows)
        assert status == "FAIL"

    def test_status_fail_value_case_insensitive(self):
        rows = [{"status": "fail"}]
        status, _ = _evaluate_result(rows)
        assert status == "FAIL"

    def test_status_pass_value_case_insensitive(self):
        rows = [{"status": "pass"}]
        status, _ = _evaluate_result(rows)
        assert status == "PASS"


# ── _extract_failure_detail ─────────────────────────────────────────────────


class TestExtractFailureDetail:
    def test_empty_rows(self):
        assert _extract_failure_detail([]) == ""

    def test_with_status_column_shows_failures(self):
        rows = [
            {"check": "a", "status": "PASS"},
            {"check": "b", "status": "FAIL"},
        ]
        detail = _extract_failure_detail(rows)
        assert "b" in detail
        assert "FAIL" in detail

    def test_without_status_column_shows_violations(self):
        rows = [{"bad_id": 1}, {"bad_id": 2}]
        detail = _extract_failure_detail(rows)
        assert "bad_id" in detail


# ── _fmt_result_rows ────────────────────────────────────────────────────────


class TestFmtResultRows:
    def test_empty(self):
        assert _fmt_result_rows([]) == "(no rows)"

    def test_single_row(self):
        result = _fmt_result_rows([{"a": 1, "b": 2}])
        assert "a=1" in result
        assert "b=2" in result

    def test_respects_limit(self):
        rows = [{"id": i} for i in range(20)]
        result = _fmt_result_rows(rows, limit=5)
        assert "15 more rows" in result

    def test_no_suffix_under_limit(self):
        rows = [{"id": 1}]
        result = _fmt_result_rows(rows, limit=10)
        assert "more rows" not in result


# ── run_custom_checks ───────────────────────────────────────────────────────


class TestRunCustomChecks:
    @patch("prelight.core.quality_checks.get_client")
    def test_passing_check(self, mock_get_client):
        client = MagicMock()
        client.execute_query.return_value = [{"status": "PASS", "drift": 2.1}]
        mock_get_client.return_value = client

        result = run_custom_checks("sbx_orders", "orders", [
            {"name": "row_count", "sql": "SELECT ...", "description": "Row count check"},
        ])
        assert result["all_passed"] is True
        assert len(result["checks"]) == 1
        assert result["checks"][0]["status"] == "PASS"

    @patch("prelight.core.quality_checks.get_client")
    def test_failing_check(self, mock_get_client):
        client = MagicMock()
        client.execute_query.return_value = [{"status": "FAIL", "drift": 55.0}]
        mock_get_client.return_value = client

        result = run_custom_checks("sbx_orders", "orders", [
            {"name": "mean_drift", "sql": "SELECT ...", "description": "Mean drift check"},
        ])
        assert result["all_passed"] is False
        assert result["checks"][0]["status"] == "FAIL"

    @patch("prelight.core.quality_checks.get_client")
    def test_violation_style_pass(self, mock_get_client):
        """Zero rows = PASS for violation-style checks."""
        client = MagicMock()
        client.execute_query.return_value = []
        mock_get_client.return_value = client

        result = run_custom_checks("sbx_orders", "orders", [
            {"name": "uniqueness", "sql": "SELECT ...", "description": "No duplicates"},
        ])
        assert result["all_passed"] is True

    @patch("prelight.core.quality_checks.get_client")
    def test_violation_style_fail(self, mock_get_client):
        """Rows without status column = violations = FAIL."""
        client = MagicMock()
        client.execute_query.return_value = [{"dup_id": 42, "count": 3}]
        mock_get_client.return_value = client

        result = run_custom_checks("sbx_orders", "orders", [
            {"name": "uniqueness", "sql": "SELECT ...", "description": "No duplicates"},
        ])
        assert result["all_passed"] is False

    @patch("prelight.core.quality_checks.get_client")
    def test_query_error_is_fail(self, mock_get_client):
        """If the check SQL itself errors, the check should FAIL."""
        client = MagicMock()
        client.execute_query.side_effect = RuntimeError("table not found")
        mock_get_client.return_value = client

        result = run_custom_checks("sbx_orders", "orders", [
            {"name": "broken", "sql": "SELECT * FROM nonexistent", "description": "Bad check"},
        ])
        assert result["all_passed"] is False
        assert result["checks"][0]["status"] == "FAIL"
        assert "table not found" in result["checks"][0]["result"]

    @patch("prelight.core.quality_checks.get_client")
    def test_mixed_pass_fail(self, mock_get_client):
        client = MagicMock()
        # First check passes, second fails
        client.execute_query.side_effect = [
            [{"status": "PASS"}],
            [{"status": "FAIL"}],
        ]
        mock_get_client.return_value = client

        result = run_custom_checks("sbx_orders", "orders", [
            {"name": "check_a", "sql": "SELECT ...", "description": "A"},
            {"name": "check_b", "sql": "SELECT ...", "description": "B"},
        ])
        assert result["all_passed"] is False
        assert result["checks"][0]["status"] == "PASS"
        assert result["checks"][1]["status"] == "FAIL"

    @patch("prelight.core.quality_checks.get_client")
    def test_run_id_is_uuid(self, mock_get_client):
        client = MagicMock()
        client.execute_query.return_value = []
        mock_get_client.return_value = client

        result = run_custom_checks("sbx_orders", "orders", [
            {"name": "test", "sql": "SELECT 1", "description": "test"},
        ])
        # UUID format: 8-4-4-4-12 hex chars
        assert len(result["run_id"]) == 36
        assert result["run_id"].count("-") == 4
