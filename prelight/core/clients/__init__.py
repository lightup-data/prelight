"""
Database client factory and config-switching utilities.

get_client() returns the active backend module (databricks_client or duckdb_client)
based on the configured backend in config.yaml. Both modules expose the same interface:

    execute_query(sql)                              -> list[dict]
    execute_statement(sql)                          -> None
    get_table_schema(table)                         -> list[dict]
    get_row_count(table)                            -> int
    list_table_names(schema)                        -> list[str]
    list_tables_with_metadata(schema, table_names)  -> list[dict]  # 2 DB calls for N tables
    create_sandbox_table(src, dst)                  -> None
    table_exists(table)                             -> bool
    reset_connection()                              -> None

reset_all() resets both the settings cache and all database connections.
This must be called after programmatically updating config.yaml so that the
next tool invocation picks up the new configuration.
"""

from __future__ import annotations

from types import ModuleType

from prelight.config.settings import get_settings


def get_client() -> ModuleType:
    """Return the database client module for the configured backend."""
    settings = get_settings()
    if settings.backend == "duckdb":
        from prelight.core.clients import duckdb_client
        return duckdb_client
    else:
        from prelight.core.clients import databricks_client
        return databricks_client


def reset_all() -> None:
    """Reset settings cache and all database connections.

    Call this after writing a new config.yaml so the next tool call
    picks up the updated backend / credentials.
    """
    from prelight.config.settings import reset_settings
    from prelight.core.clients import duckdb_client, databricks_client
    reset_settings()
    duckdb_client.reset_connection()
    databricks_client.reset_connection()
