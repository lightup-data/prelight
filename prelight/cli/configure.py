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
    """Write or update the duckdb: section, removing the databricks: section."""
    raw = _load_raw()
    raw.pop("databricks", None)
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
    """Write or update the databricks: section, removing the duckdb: section."""
    raw = _load_raw()
    raw.pop("duckdb", None)
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


