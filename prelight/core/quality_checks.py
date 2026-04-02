from __future__ import annotations

import uuid

from prelight.core.clients import get_client


def run_custom_checks(sandbox_name: str, source_table: str, custom_checks: list[dict]) -> dict:
    """Execute Claude-defined quality checks against the sandbox.

    Each check dict must have:
      name:        str  — unique identifier (used as SQL filename)
      sql:         str  — comparison query to run
      description: str  — what this check verifies (used as 'expected' in results)

    Pass/fail evaluation:
      - If the query returns 0 rows → PASS (violation-style checks)
      - If the query returns rows with a 'status' column → FAIL if any row has status='FAIL'
      - If the query returns rows without a 'status' column → FAIL (unexpected violations)

    """
    client = get_client()
    run_id = str(uuid.uuid4())
    checks: list[dict] = []

    for check in custom_checks:
        name = check.get("name", "unnamed_check")
        sql = check.get("sql", "")
        description = check.get("description", name)

        try:
            rows = client.execute_query(sql)
            status, result_str = _evaluate_result(rows)
            detail = _extract_failure_detail(rows) if status == "FAIL" else ""
        except Exception as e:
            status = "FAIL"
            result_str = f"Query error: {e}"
            detail = str(e)

        checks.append({
            "check": name,
            "status": status,
            "result": result_str,
            "expected": description,
            "detail": detail,
            "sql": sql,
        })

    all_passed = all(c["status"] == "PASS" for c in checks)
    return {
        "run_id": run_id,
        "sandbox_name": sandbox_name,
        "source_table": source_table,
        "checks": checks,
        "all_passed": all_passed,
    }


def _evaluate_result(rows: list[dict]) -> tuple[str, str]:
    """Determine pass/fail from query result rows and return a readable result string.

    Logic:
      - 0 rows returned          → PASS (no violations found)
      - rows with 'status' col   → FAIL if any row has status='FAIL', else PASS
      - rows without 'status'    → FAIL (rows themselves are violations)
    """
    if not rows:
        return "PASS", "(no violations found)"

    first_keys = {k.lower() for k in rows[0].keys()}
    if "status" in first_keys:
        status_key = next(k for k in rows[0].keys() if k.lower() == "status")
        failed_rows = [r for r in rows if str(r.get(status_key, "")).upper() == "FAIL"]
        overall = "FAIL" if failed_rows else "PASS"
    else:
        overall = "FAIL"

    return overall, _fmt_result_rows(rows)


def _extract_failure_detail(rows: list[dict]) -> str:
    """Return a concise failure summary from result rows."""
    if not rows:
        return ""
    status_key = next((k for k in rows[0].keys() if k.lower() == "status"), None)
    if status_key:
        failed = [r for r in rows if str(r.get(status_key, "")).upper() == "FAIL"]
        return _fmt_result_rows(failed, limit=3) if failed else ""
    return _fmt_result_rows(rows, limit=3)


def _fmt_result_rows(rows: list[dict], limit: int = 10) -> str:
    """Format result rows as a concise readable string."""
    if not rows:
        return "(no rows)"
    display = rows[:limit]
    parts = [", ".join(f"{k}={v}" for k, v in row.items()) for row in display]
    suffix = f" ... ({len(rows) - limit} more rows)" if len(rows) > limit else ""
    return "; ".join(parts) + suffix
