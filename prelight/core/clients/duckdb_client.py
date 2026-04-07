from __future__ import annotations

import duckdb

from prelight.config.settings import get_settings
from prelight.core.sql_utils import validate_identifier

# Single persistent connection — DuckDB does not need separate read/write connections.
# Production safety is enforced by SQL inspection (production_guard) the same way as
# Databricks single-token mode.
_connection: duckdb.DuckDBPyConnection | None = None


def reset_connection() -> None:
    """Close and clear the cached DuckDB connection. Call after config changes."""
    global _connection
    if _connection is not None:
        try:
            _connection.close()
        except Exception:
            pass
    _connection = None


def _get_connection() -> duckdb.DuckDBPyConnection:
    global _connection
    if _connection is not None:
        return _connection
    settings = get_settings()
    db = settings.duckdb
    path = db.path or ":memory:"
    try:
        _connection = duckdb.connect(path)
        return _connection
    except Exception as e:
        raise RuntimeError(
            f"❌ DuckDB connection failed: {e}. "
            "Check 'duckdb.path' in config.yaml (use a writable file path or omit for in-memory)."
        ) from e


def execute_query(sql: str) -> list[dict]:
    """Run a SELECT query and return results as a list of dicts."""
    try:
        conn = _get_connection()
        result = conn.execute(sql)
        if result.description is None:
            return []
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(f"❌ DuckDB query failed: {e}") from e


def execute_statement(sql: str) -> None:
    """Run a write/DDL statement."""
    try:
        conn = _get_connection()
        conn.execute(sql)
    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(f"❌ DuckDB statement failed: {e}") from e


def get_table_schema(table: str) -> list[dict]:
    settings = get_settings()
    schema = validate_identifier(settings.duckdb.schema, "schema")
    table = validate_identifier(table, "table")
    full_table = f"{schema}.{table}" if "." not in table else table
    rows = execute_query(f"DESCRIBE {full_table}")
    return [
        {
            "column_name": row["column_name"],
            "data_type": row["column_type"],
            "nullable": row.get("null", "YES") == "YES",
        }
        for row in rows
        if row.get("column_name")
    ]


def get_row_count(table: str) -> int:
    settings = get_settings()
    schema = validate_identifier(settings.duckdb.schema, "schema")
    table = validate_identifier(table, "table")
    full_table = f"{schema}.{table}" if "." not in table else table
    rows = execute_query(f"SELECT COUNT(*) AS cnt FROM {full_table}")
    return int(rows[0]["cnt"])


def list_table_names(schema: str) -> list[str]:
    """Return all base table names in the given schema."""
    schema = validate_identifier(schema, "schema")
    rows = execute_query(
        f"SELECT table_name FROM information_schema.tables "
        f"WHERE table_schema = '{schema}' AND table_type = 'BASE TABLE'"
    )
    return [r["table_name"] for r in rows if r.get("table_name")]


def create_sandbox_table(source_table: str, sandbox_table: str) -> None:
    """Clone a production table into a sandbox table.

    Ensures the target schema exists first — DuckDB requires the schema to be
    created before tables can be added to it.
    """
    settings = get_settings()
    schema = validate_identifier(settings.duckdb.schema, "schema")
    for part in source_table.split("."):
        validate_identifier(part, "source table")
    for part in sandbox_table.split("."):
        validate_identifier(part, "sandbox table")
    execute_statement(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    execute_statement(
        f"CREATE TABLE {sandbox_table} AS SELECT * FROM {source_table}"
    )


def list_tables_with_metadata(schema: str, table_names: list[str]) -> list[dict]:
    """Return schema + row count for every table in table_names in 2 DB calls.

    Returns a list of dicts ordered by table name:
        [{"table_name": str, "row_count": int, "columns": [{"column_name", "data_type"}]}]
    """
    if not table_names:
        return []
    schema = validate_identifier(schema, "schema")
    for t in table_names:
        validate_identifier(t, "table")

    # Call 1: fetch all column metadata in one query
    quoted = ", ".join(f"'{t}'" for t in table_names)
    col_rows = execute_query(
        f"SELECT table_name, column_name, data_type "
        f"FROM information_schema.columns "
        f"WHERE table_schema = '{schema}' AND table_name IN ({quoted}) "
        f"ORDER BY table_name, ordinal_position"
    )
    columns_by_table: dict[str, list[dict]] = {t: [] for t in table_names}
    for row in col_rows:
        tname = row["table_name"]
        if tname in columns_by_table:
            columns_by_table[tname].append(
                {"column_name": row["column_name"], "data_type": row["data_type"]}
            )

    # Call 2: fetch all row counts in one UNION ALL query
    union_parts = [
        f"SELECT '{t}' AS table_name, COUNT(*) AS row_count FROM {schema}.{t}"
        for t in table_names
    ]
    count_rows = execute_query(" UNION ALL ".join(union_parts))
    counts = {r["table_name"]: int(r["row_count"]) for r in count_rows}

    return [
        {
            "table_name": t,
            "row_count": counts.get(t, 0),
            "columns": columns_by_table.get(t, []),
        }
        for t in sorted(table_names)
    ]


def table_exists(table: str) -> bool:
    settings = get_settings()
    schema = validate_identifier(settings.duckdb.schema, "schema")
    table = validate_identifier(table, "table")
    try:
        rows = execute_query(
            f"SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = '{schema}' AND table_name = '{table}'"
        )
        return len(rows) > 0
    except Exception:
        return False
