"""Shared fixtures for the Prelight test suite."""

from __future__ import annotations

import os
import textwrap
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def _isolate_settings():
    """Reset the cached settings singleton before and after every test."""
    from prelight.config.settings import reset_settings
    reset_settings()
    yield
    reset_settings()


@pytest.fixture()
def tmp_config(tmp_path: Path):
    """Write a minimal DuckDB config.yaml and point PRELIGHT_CONFIG at it.

    Returns the config file path. The DuckDB file is in-memory so no disk I/O.
    """
    config_file = tmp_path / "config.yaml"
    config_file.write_text(textwrap.dedent("""\
        duckdb:
          schema: "analytics"
          sandbox_prefix: "sbx_"
          audit_table: "qg_quality_results"
    """))
    old = os.environ.get("PRELIGHT_CONFIG")
    os.environ["PRELIGHT_CONFIG"] = str(config_file)
    yield config_file
    if old is None:
        os.environ.pop("PRELIGHT_CONFIG", None)
    else:
        os.environ["PRELIGHT_CONFIG"] = old


@pytest.fixture()
def duckdb_config_with_path(tmp_path: Path):
    """Write a DuckDB config that uses a real file (for integration tests)."""
    db_path = str(tmp_path / "test.duckdb")
    config_file = tmp_path / "config.yaml"
    config_file.write_text(textwrap.dedent(f"""\
        duckdb:
          path: "{db_path}"
          schema: "analytics"
          sandbox_prefix: "sbx_"
          audit_table: "qg_quality_results"
    """))
    old = os.environ.get("PRELIGHT_CONFIG")
    os.environ["PRELIGHT_CONFIG"] = str(config_file)
    yield config_file, db_path
    if old is None:
        os.environ.pop("PRELIGHT_CONFIG", None)
    else:
        os.environ["PRELIGHT_CONFIG"] = old
