#!/usr/bin/env python3
"""
Lightup — DuckDB Local Setup Script
====================================
Creates a local DuckDB database file and loads the demo data (orders + customers).

Usage:
    python setup/duckdb/init_local.py                    # uses config.yaml in CWD
    python setup/duckdb/init_local.py --path mydb.duckdb # override file path
    python setup/duckdb/init_local.py --path mydb.duckdb --schema my_schema

The script reads config.yaml (or PRELIGHT_CONFIG env var) to determine the
default database path and schema name, but both can be overridden via flags.

After running this script:
  - Run `uv run prelight` to start the MCP server
  - In Claude Desktop / Claude Code, type: List my tables
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


def _load_duckdb_config() -> tuple[str | None, str]:
    """Read path and schema from config.yaml if it exists. Returns (path, schema)."""
    config_path = Path(os.environ.get("PRELIGHT_CONFIG", "config.yaml"))
    if not config_path.exists():
        return None, "analytics"

    try:
        import yaml
        raw = yaml.safe_load(config_path.read_text())
        if isinstance(raw, dict) and isinstance(raw.get("duckdb"), dict):
            db = raw["duckdb"]
            return db.get("path"), db.get("schema", "analytics")
    except Exception:
        pass

    return None, "analytics"


def run_init(db_path: str | None, schema: str) -> None:
    try:
        import duckdb
    except ImportError:
        print("❌ duckdb package not found. Run: uv sync")
        sys.exit(1)

    resolved_path = db_path or ":memory:"
    display_path = db_path if db_path else "(in-memory — data will not persist)"

    print(f"\n=== Lightup — DuckDB Local Setup ===\n")
    print(f"  Database : {display_path}")
    print(f"  Schema   : {schema}")
    print()

    if db_path:
        path_obj = Path(db_path)
        path_obj.parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(resolved_path)

    sql_path = Path(__file__).parent / "demo_data.sql"
    if not sql_path.exists():
        print(f"❌ Could not find demo SQL at {sql_path}")
        sys.exit(1)

    raw_sql = sql_path.read_text(encoding="utf-8")

    # Substitute schema name if different from the default
    if schema != "analytics":
        raw_sql = raw_sql.replace("analytics", schema)

    statements = [
        s.strip() for s in raw_sql.split(";")
        if s.strip() and not all(
            line.startswith("--") or not line.strip()
            for line in s.strip().splitlines()
        )
    ]

    failed = 0
    for i, stmt in enumerate(statements, 1):
        label = stmt.splitlines()[0][:80]
        try:
            conn.execute(stmt)
            print(f"  [{i}/{len(statements)}] ✅  {label}")
        except Exception as e:
            print(f"  [{i}/{len(statements)}] ❌  {label}")
            print(f"       Error: {e}")
            failed += 1

    print()
    if failed:
        print(f"⚠️  {failed} statement(s) failed. Check errors above.")
    else:
        try:
            result = conn.execute(
                f"SELECT 'customers' AS t, COUNT(*) AS n FROM {schema}.customers "
                f"UNION ALL "
                f"SELECT 'orders', COUNT(*) FROM {schema}.orders"
            ).fetchall()
            print("✅ Demo data loaded successfully!\n")
            print("  Table       Rows")
            print("  ──────────  ────")
            for t, n in result:
                print(f"  {t:<10}  {n}")
        except Exception:
            print("✅ Demo data loaded successfully!")

    conn.close()

    print()
    print("  Next steps:")
    print("    1. Make sure config.yaml has:")
    if db_path:
        print(f'         duckdb:')
        print(f'           path: "{db_path}"')
        print(f'           schema: "{schema}"')
    else:
        print(f'         duckdb:')
        print(f'           schema: "{schema}"')
    print("    2. Run: uv run prelight install")
    print("    3. Restart Claude Desktop, then type: List my tables")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Initialize a local DuckDB database with Lightup demo data."
    )
    parser.add_argument(
        "--path",
        default=None,
        help="Path to the .duckdb file (default: read from config.yaml, or in-memory if not set)",
    )
    parser.add_argument(
        "--schema",
        default=None,
        help="Schema name (default: read from config.yaml, fallback to 'analytics')",
    )
    args = parser.parse_args()

    config_path, config_schema = _load_duckdb_config()

    db_path = args.path or config_path
    schema = args.schema or config_schema

    run_init(db_path, schema)


if __name__ == "__main__":
    main()
