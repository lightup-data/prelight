from __future__ import annotations

from prelight.config.settings import get_settings
from prelight.core.sql_utils import validate_identifier

try:
    import psycopg2
    import psycopg2.extras
except ImportError as _e:
    raise ImportError(
        "psycopg2 is required for PostgreSQL support. "
        "Install it with: pip install 'prelight[postgres]'"
    ) from _e

# Single persistent connection — PostgreSQL does not need separate read/write connections.
# Production safety is enforced by SQL inspection (production_guard).
_connection = None


def reset_connection() -> None:
    """Close and clear the cached PostgreSQL connection. Call after config changes."""
    global _connection
    if _connection is not None:
        try:
            _connection.close()
        except Exception:
            pass
    _connection = None


def _get_connection():
    global _connection
    if _connection is not None:
        try:
            # Reconnect if the connection was dropped
            _connection.isolation_level
        except Exception:
            _connection = None
    if _connection is not None:
        return _connection
    settings = get_settings()
    pg = settings.postgres
    try:
        _connection = psycopg2.connect(
            host=pg.host,
            port=pg.port,
            dbname=pg.database,
            user=pg.user,
            password=pg.password,
            sslmode=pg.ssl_mode,
        )
        _connection.autocommit = True
        return _connection
    except Exception as e:
        raise RuntimeError(
            f"❌ PostgreSQL connection failed: {e}. "
            "Check host/port/database/user/password in config.yaml"
        ) from e


def execute_query(sql: str) -> list[dict]:
    """Run a SELECT query and return results as a list of dicts."""
    try:
        conn = _get_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql)
            if cur.description is None:
                return []
            return [dict(row) for row in cur.fetchall()]
    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(f"❌ PostgreSQL query failed: {e}") from e


def execute_statement(sql: str) -> None:
    """Run a write/DDL statement."""
    try:
        conn = _get_connection()
        with conn.cursor() as cur:
            cur.execute(sql)
    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(f"❌ PostgreSQL statement failed: {e}") from e


def get_table_schema(table: str) -> list[dict]:
    settings = get_settings()
    schema = validate_identifier(settings.postgres.schema, "schema")
    table = validate_identifier(table, "table")
    rows = execute_query(
        f"SELECT column_name, data_type, is_nullable "
        f"FROM information_schema.columns "
        f"WHERE table_schema = '{schema}' AND table_name = '{table}' "
        f"ORDER BY ordinal_position"
    )
    return [
        {
            "column_name": row["column_name"],
            "data_type": row["data_type"],
            "nullable": row["is_nullable"] == "YES",
        }
        for row in rows
    ]


def get_row_count(table: str) -> int:
    settings = get_settings()
    schema = validate_identifier(settings.postgres.schema, "schema")
    table = validate_identifier(table, "table")
    rows = execute_query(f'SELECT COUNT(*) AS cnt FROM "{schema}"."{table}"')
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
    """Clone a production table into a sandbox table."""
    settings = get_settings()
    schema = validate_identifier(settings.postgres.schema, "schema")
    for part in source_table.split("."):
        validate_identifier(part, "source table")
    for part in sandbox_table.split("."):
        validate_identifier(part, "sandbox table")
    # Ensure schema exists
    execute_statement(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    execute_statement(
        f"CREATE TABLE {sandbox_table} AS SELECT * FROM {source_table}"
    )


def list_tables_with_metadata(schema: str, table_names: list[str]) -> list[dict]:
    """Return schema + row count for every table in table_names in 2 DB calls."""
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
        f'SELECT \'{t}\' AS table_name, COUNT(*) AS row_count FROM "{schema}"."{t}"'
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
    schema = validate_identifier(settings.postgres.schema, "schema")
    table = validate_identifier(table, "table")
    try:
        rows = execute_query(
            f"SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = '{schema}' AND table_name = '{table}'"
        )
        return len(rows) > 0
    except Exception:
        return False
