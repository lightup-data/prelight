"""Tests for config/settings: validation, loading, and error handling."""

from __future__ import annotations

import os
import textwrap
from pathlib import Path

import pytest

from prelight.config.settings import (
    DatabricksConfig,
    DuckDBConfig,
    QualityConfig,
    Settings,
    get_settings,
    reset_settings,
)


# ── DuckDBConfig ────────────────────────────────────────────────────────────


class TestDuckDBConfig:
    def test_defaults(self):
        cfg = DuckDBConfig()
        assert cfg.path is None
        assert cfg.schema == "analytics"
        assert cfg.sandbox_prefix == "sbx_"
        assert cfg.audit_table == "qg_quality_results"

    def test_custom_values(self):
        cfg = DuckDBConfig(path="/tmp/test.duckdb", schema="myschema")
        assert cfg.path == "/tmp/test.duckdb"
        assert cfg.schema == "myschema"

    def test_empty_schema_rejected(self):
        with pytest.raises(Exception):
            DuckDBConfig(schema="")

    def test_whitespace_schema_rejected(self):
        with pytest.raises(Exception):
            DuckDBConfig(schema="   ")


# ── DatabricksConfig ────────────────────────────────────────────────────────


class TestDatabricksConfig:
    def _valid_config(self, **overrides) -> dict:
        defaults = {
            "host": "https://adb-1234567890.azuredatabricks.net",
            "http_path": "/sql/1.0/warehouses/abc123",
            "schema": "analytics",
            "token": "dapi_test_token",
        }
        defaults.update(overrides)
        return defaults

    def test_single_token_mode(self):
        cfg = DatabricksConfig(**self._valid_config())
        assert cfg.effective_prod_token == "dapi_test_token"
        assert cfg.effective_sandbox_token == "dapi_test_token"
        assert cfg.dual_token_mode is False

    def test_dual_token_mode(self):
        cfg = DatabricksConfig(**self._valid_config(
            token=None,
            prod_token="read_token",
            sandbox_token="write_token",
        ))
        assert cfg.effective_prod_token == "read_token"
        assert cfg.effective_sandbox_token == "write_token"
        assert cfg.dual_token_mode is True

    def test_no_token_rejected(self):
        with pytest.raises(Exception, match="token"):
            DatabricksConfig(**self._valid_config(token=None))

    def test_host_must_be_https(self):
        with pytest.raises(Exception, match="https"):
            DatabricksConfig(**self._valid_config(host="http://bad.example.com"))

    def test_empty_http_path_rejected(self):
        with pytest.raises(Exception):
            DatabricksConfig(**self._valid_config(http_path=""))

    def test_empty_schema_rejected(self):
        with pytest.raises(Exception):
            DatabricksConfig(**self._valid_config(schema=""))


# ── Settings (top-level) ────────────────────────────────────────────────────


class TestSettings:
    def test_duckdb_backend(self):
        s = Settings(duckdb=DuckDBConfig())
        assert s.backend == "duckdb"
        assert s.db_schema == "analytics"
        assert s.sandbox_prefix == "sbx_"

    def test_databricks_backend(self):
        s = Settings(databricks=DatabricksConfig(
            host="https://example.net",
            http_path="/sql/1.0/warehouses/abc",
            schema="prod",
            token="tok",
        ))
        assert s.backend == "databricks"
        assert s.db_schema == "prod"

    def test_no_backend_rejected(self):
        with pytest.raises(Exception, match="No database backend"):
            Settings()

    def test_both_backends_rejected(self):
        with pytest.raises(Exception, match="exactly one"):
            Settings(
                duckdb=DuckDBConfig(),
                databricks=DatabricksConfig(
                    host="https://example.net",
                    http_path="/sql/1.0/warehouses/abc",
                    schema="prod",
                    token="tok",
                ),
            )


# ── get_settings from config file ──────────────────────────────────────────


class TestGetSettings:
    def test_loads_valid_config(self, tmp_config):
        settings = get_settings()
        assert settings.backend == "duckdb"
        assert settings.db_schema == "analytics"

    def test_missing_config_raises_runtime_error(self, tmp_path):
        """Must raise RuntimeError (not sys.exit) when config is missing."""
        os.environ["PRELIGHT_CONFIG"] = str(tmp_path / "nonexistent.yaml")
        reset_settings()
        with pytest.raises(RuntimeError, match="config.yaml not found"):
            get_settings()

    def test_empty_config_raises_runtime_error(self, tmp_path):
        config_file = tmp_path / "config.yaml"
        config_file.write_text("")
        os.environ["PRELIGHT_CONFIG"] = str(config_file)
        reset_settings()
        with pytest.raises(RuntimeError, match="empty"):
            get_settings()

    def test_invalid_yaml_raises_runtime_error(self, tmp_path):
        config_file = tmp_path / "config.yaml"
        config_file.write_text(": : : invalid yaml [")
        os.environ["PRELIGHT_CONFIG"] = str(config_file)
        reset_settings()
        with pytest.raises(RuntimeError, match="not valid YAML"):
            get_settings()

    def test_invalid_config_values_raises_runtime_error(self, tmp_path):
        config_file = tmp_path / "config.yaml"
        config_file.write_text(textwrap.dedent("""\
            databricks:
              host: "not-https"
              http_path: ""
              schema: "analytics"
              token: "tok"
        """))
        os.environ["PRELIGHT_CONFIG"] = str(config_file)
        reset_settings()
        with pytest.raises(RuntimeError, match="validation failed"):
            get_settings()

    def test_settings_are_cached(self, tmp_config):
        s1 = get_settings()
        s2 = get_settings()
        assert s1 is s2

    def test_reset_clears_cache(self, tmp_config):
        s1 = get_settings()
        reset_settings()
        s2 = get_settings()
        assert s1 is not s2
