from __future__ import annotations

from databricks import sql as dbsql

from prelight.config.settings import get_settings
from prelight.core.sql_utils import validate_identifier

# Two separate connections:
#   _prod_connection    — uses prod_token (read-only); for SELECT queries on production tables
#   _sandbox_connection — uses sandbox_token (write-enabled); for sandbox writes + audit writes
# In single-token legacy mode both point to the same underlying PAT.
_prod_connection = None
_sandbox_connection = None


def reset_connection() -> None:
    """Close and clear cached Databricks connections. Call after config changes."""
    global _prod_connection, _sandbox_connection
    for conn in [_prod_connection, _sandbox_connection]:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
    _prod_connection = None
    _sandbox_connection = None


def _make_connection(token: str):
    settings = get_settings()
    db = settings.databricks
    hostname = db.host.replace("https://", "").rstrip("/")
    try:
        return dbsql.connect(
            server_hostname=hostname,
            http_path=db.http_path,
            access_token=token,
            catalog=db.catalog,
        )
    except Exception as e:
        raise RuntimeError(
            f"❌ Databricks connection failed: {e}. "
            "Check host/token/http_path in config.yaml"
        ) from e


def get_prod_connection():
    global _prod_connection
    if _prod_connection is not None:
        return _prod_connection
    settings = get_settings()
    _prod_connection = _make_connection(settings.databricks.effective_prod_token)
    return _prod_connection


def get_sandbox_connection():
    global _sandbox_connection
    if _sandbox_connection is not None:
        return _sandbox_connection
    settings = get_settings()
    _sandbox_connection = _make_connection(settings.databricks.effective_sandbox_token)
    return _sandbox_connection


def execute_query(sql: str) -> list[dict]:
    """Run a SELECT query using the production (read-only) connection."""
    try:
        conn = get_prod_connection()
        with conn.cursor() as cursor:
            cursor.execute(sql)
            if cursor.description is None:
                return []
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(f"❌ Databricks query failed: {e}") from e


def execute_statement(sql: str) -> None:
    """Run a write statement using the sandbox (write-enabled) connection."""
    try:
        conn = get_sandbox_connection()
        with conn.cursor() as cursor:
            cursor.execute(sql)
    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(f"❌ Databricks statement failed: {e}") from e


def get_table_schema(table: str) -> list[dict]:
    settings = get_settings()
    schema = validate_identifier(settings.databricks.schema, "schema")
    table = validate_identifier(table, "table")
    full_table = f"{schema}.{table}" if "." not in table else table
    rows = execute_query(f"DESCRIBE TABLE {full_table}")
    result = []
    for row in rows:
        col_name = row.get("col_name", "")
        if not col_name or not col_name.strip() or col_name.strip().startswith("#"):
            break
        result.append(
            {
                "column_name": col_name.strip(),
                "data_type": row.get("data_type", "").strip(),
                "nullable": True,
            }
        )
    return result


def get_row_count(table: str) -> int:
    settings = get_settings()
    schema = validate_identifier(settings.databricks.schema, "schema")
    table = validate_identifier(table, "table")
    full_table = f"{schema}.{table}" if "." not in table else table
    rows = execute_query(f"SELECT COUNT(*) AS cnt FROM {full_table}")
    return int(rows[0]["cnt"])


def list_table_names(schema: str) -> list[str]:
    """Return all table names in the given schema."""
    schema = validate_identifier(schema, "schema")
    rows = execute_query(f"SHOW TABLES IN {schema}")
    return [
        r.get("tableName", r.get("table_name", ""))
        for r in rows
        if r.get("tableName", r.get("table_name", ""))
    ]


def create_sandbox_table(source_table: str, sandbox_table: str) -> None:
    """Clone a production table into a sandbox using the sandbox connection."""
    validate_identifier(source_table.split(".")[-1], "source table")
    validate_identifier(sandbox_table.split(".")[-1], "sandbox table")
    execute_statement(
        f"CREATE TABLE {sandbox_table} AS SELECT * FROM {source_table}"
    )


def table_exists(table: str) -> bool:
    settings = get_settings()
    schema = validate_identifier(settings.databricks.schema, "schema")
    table = validate_identifier(table, "table")
    try:
        rows = execute_query(f"SHOW TABLES IN {schema} LIKE '{table}'")
        return len(rows) > 0
    except Exception:
        return False
