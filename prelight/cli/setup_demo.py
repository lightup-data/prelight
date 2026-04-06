"""
prelight setup-demo

Detects the configured backend from config.yaml and runs the appropriate demo
data setup:
  - Databricks: executes setup/databricks/demo_data.sql via the Databricks connection
  - DuckDB:     executes setup/duckdb/demo_data.sql via the local DuckDB connection

Requires config.yaml to exist (run `prelight install` first).
"""

from __future__ import annotations

import sys
from pathlib import Path


def run_setup_demo_core() -> str:
    """Execute the demo data SQL for the configured backend and return a result string.
    Used by both the CLI command and the MCP setup_demo tool.
    """
    from prelight.config.settings import get_settings
    settings = get_settings()

    if settings.backend == "duckdb":
        return _run_duckdb_setup(settings)
    else:
        return _run_databricks_setup(settings)


def _run_databricks_setup(settings) -> str:
    from prelight.core.clients.databricks_client import execute_statement, execute_query

    schema = settings.db_schema
    sql_path = Path(__file__).parent.parent.parent / "setup" / "databricks" / "demo_data.sql"
    if not sql_path.exists():
        return f"❌ Could not find Databricks demo SQL at {sql_path}"

    raw_sql = sql_path.read_text(encoding="utf-8")
    statements = _split_statements(raw_sql)

    failed = 0
    for stmt in statements:
        try:
            execute_statement(stmt)
        except Exception:
            failed += 1

    if failed:
        return f"⚠️ Demo setup: {failed} of {len(statements)} statement(s) failed."

    try:
        rows = execute_query(
            f"SELECT 'customers' AS t, COUNT(*) AS n FROM {schema}.customers "
            f"UNION ALL "
            f"SELECT 'orders', COUNT(*) FROM {schema}.orders"
        )
        counts = ", ".join(f"{r['t']} ({r['n']} rows)" for r in rows)
        return f"✅ Demo data ready in {schema}: {counts}. Say: List my tables"
    except Exception:
        return f"✅ Demo data setup complete in {schema}. Say: List my tables"


def _run_duckdb_setup(settings) -> str:
    from prelight.core.clients.duckdb_client import execute_statement, execute_query

    schema = settings.db_schema
    display_path = settings.duckdb.path or "(in-memory)"

    sql_path = Path(__file__).parent.parent.parent / "setup" / "duckdb" / "demo_data.sql"
    if not sql_path.exists():
        return f"❌ Could not find DuckDB demo SQL at {sql_path}"

    raw_sql = sql_path.read_text(encoding="utf-8")

    # Substitute schema name if it differs from the default in the SQL file
    if schema != "analytics":
        raw_sql = raw_sql.replace("analytics", schema)

    statements = _split_statements(raw_sql)

    failed = 0
    for stmt in statements:
        try:
            execute_statement(stmt)
        except Exception:
            failed += 1

    if failed:
        return f"⚠️ Demo setup: {failed} of {len(statements)} statement(s) failed."

    try:
        rows = execute_query(
            f"SELECT 'customers' AS t, COUNT(*) AS n FROM {schema}.customers "
            f"UNION ALL "
            f"SELECT 'orders', COUNT(*) FROM {schema}.orders"
        )
        counts = ", ".join(f"{r['t']} ({r['n']} rows)" for r in rows)
        return f"✅ Demo data ready in {schema} ({display_path}): {counts}. Say: List my tables"
    except Exception:
        return f"✅ Demo data setup complete in {schema}. Say: List my tables"


def _split_statements(raw_sql: str) -> list[str]:
    """Split a SQL file into individual statements, skipping comment-only blocks."""
    return [
        s.strip() for s in raw_sql.split(";")
        if s.strip() and not all(
            line.startswith("--") or not line.strip()
            for line in s.strip().splitlines()
        )
    ]


def run_setup_demo() -> None:
    """CLI entry point — prints output to stdout."""
    print("\n=== Prelight — Demo Data Setup ===\n")
    print(run_setup_demo_core())
    print()
