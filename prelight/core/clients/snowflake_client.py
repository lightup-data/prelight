from __future__ import annotations

from prelight.config.settings import get_settings
from prelight.core.sql_utils import validate_identifier

try:
    import snowflake.connector
except ImportError as _e:
    raise ImportError(
        "snowflake-connector-python is required for Snowflake support. "
        "Install it with: pip install 'prelight[snowflake]'"
    ) from _e

# Single persistent connection — production safety is enforced by SQL inspection.
_connection = None


def reset_connection() -> None:
    """Close and clear the cached Snowflake connection. Call after config changes."""
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
            if _connection.is_closed():
                _connection = None
        except Exception:
            _connection = None
    if _connection is not None:
        return _connection
    settings = get_settings()
    sf = settings.snowflake
    kwargs: dict = {
        "account": sf.account,
        "user": sf.user,
        "password": sf.password,
        "warehouse": sf.warehouse,
        "database": sf.database,
        "schema": sf.schema,
    }
    if sf.role:
        kwargs["role"] = sf.role
    try:
        _connection = snowflake.connector.connect(**kwargs)
        return _connection
    except Exception as e:
        raise RuntimeError(
            f"❌ Snowflake connection failed: {e}. "
            "Check account/user/password/warehouse/database in config.yaml"
        ) from e


def execute_query(sql: str) -> list[dict]:
    """Run a SELECT query and return results as a list of dicts."""
    try:
        conn = _get_connection()
        cur = conn.cursor(snowflake.connector.DictCursor)
        try:
            cur.execute(sql)
            if cur.description is None:
                return []
            return cur.fetchall()
        finally:
            cur.close()
    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(f"❌ Snowflake query failed: {e}") from e


def execute_statement(sql: str) -> None:
    """Run a write/DDL statement."""
    try:
        conn = _get_connection()
        cur = conn.cursor()
        try:
            cur.execute(sql)
        finally:
            cur.close()
    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(f"❌ Snowflake statement failed: {e}") from e


def get_table_schema(table: str) -> list[dict]:
    settings = get_settings()
    schema = validate_identifier(settings.snowflake.schema, "schema")
    table = validate_identifier(table, "table")
    rows = execute_query(
        f"SELECT column_name, data_type, is_nullable "
        f"FROM information_schema.columns "
        f"WHERE table_schema = '{schema.upper()}' AND table_name = '{table.upper()}' "
        f"ORDER BY ordinal_position"
    )
    return [
        {
            "column_name": row["COLUMN_NAME"],
            "data_type": row["DATA_TYPE"],
            "nullable": row["IS_NULLABLE"] == "YES",
        }
        for row in rows
    ]


def get_row_count(table: str) -> int:
    settings = get_settings()
    schema = validate_identifier(settings.snowflake.schema, "schema")
    table = validate_identifier(table, "table")
    rows = execute_query(f"SELECT COUNT(*) AS cnt FROM {schema}.{table}")
    return int(rows[0]["CNT"])


def list_table_names(schema: str) -> list[str]:
    """Return all base table names in the given schema."""
    schema = validate_identifier(schema, "schema")
    rows = execute_query(
        f"SELECT table_name FROM information_schema.tables "
        f"WHERE table_schema = '{schema.upper()}' AND table_type = 'BASE TABLE'"
    )
    return [r["TABLE_NAME"] for r in rows if r.get("TABLE_NAME")]


def create_sandbox_table(source_table: str, sandbox_table: str) -> None:
    """Clone a production table into a sandbox table."""
    for part in source_table.split("."):
        validate_identifier(part, "source table")
    for part in sandbox_table.split("."):
        validate_identifier(part, "sandbox table")
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
    quoted = ", ".join(f"'{t.upper()}'" for t in table_names)
    col_rows = execute_query(
        f"SELECT table_name, column_name, data_type "
        f"FROM information_schema.columns "
        f"WHERE table_schema = '{schema.upper()}' AND table_name IN ({quoted}) "
        f"ORDER BY table_name, ordinal_position"
    )
    columns_by_table: dict[str, list[dict]] = {t.upper(): [] for t in table_names}
    for row in col_rows:
        tname = row["TABLE_NAME"]
        if tname in columns_by_table:
            columns_by_table[tname].append(
                {"column_name": row["COLUMN_NAME"], "data_type": row["DATA_TYPE"]}
            )

    # Call 2: fetch all row counts in one UNION ALL query
    union_parts = [
        f"SELECT '{t.upper()}' AS table_name, COUNT(*) AS row_count FROM {schema}.{t}"
        for t in table_names
    ]
    count_rows = execute_query(" UNION ALL ".join(union_parts))
    counts = {r["TABLE_NAME"]: int(r["ROW_COUNT"]) for r in count_rows}

    # Return using original case for table names
    return [
        {
            "table_name": t,
            "row_count": counts.get(t.upper(), 0),
            "columns": columns_by_table.get(t.upper(), []),
        }
        for t in sorted(table_names)
    ]


def table_exists(table: str) -> bool:
    settings = get_settings()
    schema = validate_identifier(settings.snowflake.schema, "schema")
    table = validate_identifier(table, "table")
    try:
        rows = execute_query(
            f"SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = '{schema.upper()}' AND table_name = '{table.upper()}'"
        )
        return len(rows) > 0
    except Exception:
        return False
