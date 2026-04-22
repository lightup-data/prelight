"""
Config update helpers used by the configure_* MCP tools.

Each function merges the new backend/github section into the existing config.yaml,
preserving all other sections (quality, the other backend, etc.).
"""

from __future__ import annotations

import os
from pathlib import Path

import yaml


def _config_path() -> Path:
    env = os.environ.get("PRELIGHT_CONFIG", "").strip()
    return Path(env) if env else Path("config.yaml")


def _load_raw() -> dict:
    p = _config_path()
    if p.exists():
        raw = yaml.safe_load(p.read_text()) or {}
        return raw if isinstance(raw, dict) else {}
    return {}


def _save_raw(data: dict) -> None:
    p = _config_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(yaml.dump(data, default_flow_style=False, allow_unicode=True), encoding="utf-8")


def apply_duckdb(path: str | None, schema: str, sandbox_prefix: str, audit_table: str) -> Path:
    """Write or update the duckdb: section, removing all other backend sections."""
    raw = _load_raw()
    for key in ("databricks", "postgres", "snowflake"):
        raw.pop(key, None)
    duckdb_cfg: dict = {"schema": schema, "sandbox_prefix": sandbox_prefix, "audit_table": audit_table}
    if path:
        duckdb_cfg = {"path": path, **duckdb_cfg}
    raw["duckdb"] = duckdb_cfg
    _save_raw(raw)
    return _config_path()


def apply_databricks(
    host: str,
    http_path: str,
    schema: str,
    token: str | None,
    prod_token: str | None,
    sandbox_token: str | None,
    sandbox_prefix: str,
    audit_table: str,
    catalog: str,
) -> Path:
    """Write or update the databricks: section, removing all other backend sections."""
    raw = _load_raw()
    for key in ("duckdb", "postgres", "snowflake"):
        raw.pop(key, None)
    db_cfg: dict = {
        "host": host,
        "http_path": http_path,
        "schema": schema,
        "catalog": catalog,
        "sandbox_prefix": sandbox_prefix,
        "audit_table": audit_table,
    }
    if prod_token and sandbox_token:
        db_cfg["prod_token"] = prod_token
        db_cfg["sandbox_token"] = sandbox_token
    else:
        db_cfg["token"] = token
    raw["databricks"] = db_cfg
    _save_raw(raw)
    return _config_path()


def apply_postgres(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    schema: str,
    ssl_mode: str,
    sandbox_prefix: str,
    audit_table: str,
) -> Path:
    """Write or update the postgres: section, removing all other backend sections."""
    raw = _load_raw()
    for key in ("duckdb", "databricks", "snowflake"):
        raw.pop(key, None)
    raw["postgres"] = {
        "host": host,
        "port": port,
        "database": database,
        "user": user,
        "password": password,
        "schema": schema,
        "ssl_mode": ssl_mode,
        "sandbox_prefix": sandbox_prefix,
        "audit_table": audit_table,
    }
    _save_raw(raw)
    return _config_path()


def apply_snowflake(
    account: str,
    user: str,
    password: str,
    warehouse: str,
    database: str,
    schema: str,
    role: str | None,
    sandbox_prefix: str,
    audit_table: str,
) -> Path:
    """Write or update the snowflake: section, removing all other backend sections."""
    raw = _load_raw()
    for key in ("duckdb", "databricks", "postgres"):
        raw.pop(key, None)
    sf_cfg: dict = {
        "account": account,
        "user": user,
        "password": password,
        "warehouse": warehouse,
        "database": database,
        "schema": schema,
        "sandbox_prefix": sandbox_prefix,
        "audit_table": audit_table,
    }
    if role:
        sf_cfg["role"] = role
    raw["snowflake"] = sf_cfg
    _save_raw(raw)
    return _config_path()


