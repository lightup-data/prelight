import os
import sys
import warnings
from pathlib import Path

import yaml
from pydantic import BaseModel, field_validator, model_validator, ValidationError

# Pydantic v2 warns when a field name shadows a BaseModel classmethod.
# `schema` is a valid field name in v2 (model_json_schema() replaced schema()).
warnings.filterwarnings(
    "ignore",
    message=r'Field name "schema".*shadows',
    category=UserWarning,
)


def _find_config_path() -> Path:
    """
    Resolve config.yaml location.

    Priority:
      1. PRELIGHT_CONFIG env var (absolute path) — set this in Claude Desktop config
      2. config.yaml in the current working directory
    """
    env = os.environ.get("PRELIGHT_CONFIG", "").strip()
    if env:
        return Path(env)
    return Path("config.yaml")


class DatabricksConfig(BaseModel):
    host: str
    http_path: str
    schema: str
    catalog: str = "hive_metastore"
    sandbox_prefix: str = "sbx_"
    audit_table: str = "qg_quality_results"

    # Dual-token mode (recommended): prod_token is read-only, sandbox_token is write-enabled
    # for sandbox tables only — protection enforced by Databricks credentials.
    prod_token: str | None = None
    sandbox_token: str | None = None

    # Legacy single-token mode: one PAT used for both; protection enforced by SQL guard only.
    token: str | None = None

    @field_validator("host")
    @classmethod
    def host_must_be_https(cls, v: str) -> str:
        if not v.startswith("https://"):
            raise ValueError("must start with https://")
        return v

    @field_validator("http_path", "schema")
    @classmethod
    def must_be_nonempty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("must be non-empty")
        return v

    @model_validator(mode="after")
    def check_tokens(self) -> "DatabricksConfig":
        has_dual = bool(self.prod_token and self.sandbox_token)
        has_single = bool(self.token)
        if not has_dual and not has_single:
            raise ValueError(
                "Databricks token config missing. "
                "Provide prod_token + sandbox_token (dual-token mode, recommended) "
                "or token (single-token legacy mode)."
            )
        return self

    @property
    def effective_prod_token(self) -> str:
        """Token used for read-only production queries."""
        return self.prod_token or self.token  # type: ignore[return-value]

    @property
    def effective_sandbox_token(self) -> str:
        """Token used for sandbox write operations."""
        return self.sandbox_token or self.token  # type: ignore[return-value]

    @property
    def dual_token_mode(self) -> bool:
        """True when separate prod and sandbox tokens are configured."""
        return bool(self.prod_token and self.sandbox_token)


class DuckDBConfig(BaseModel):
    path: str | None = None  # absolute/relative path to .duckdb file; None = in-memory
    schema: str = "analytics"
    sandbox_prefix: str = "sbx_"
    audit_table: str = "qg_quality_results"

    @field_validator("schema")
    @classmethod
    def schema_must_be_nonempty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("must be non-empty")
        return v


class QualityConfig(BaseModel):
    row_count_drift_pct: int = 5


class Settings(BaseModel):
    databricks: DatabricksConfig | None = None
    duckdb: DuckDBConfig | None = None
    quality: QualityConfig = QualityConfig()

    @model_validator(mode="after")
    def check_exactly_one_backend(self) -> "Settings":
        has_databricks = self.databricks is not None
        has_duckdb = self.duckdb is not None
        if not has_databricks and not has_duckdb:
            raise ValueError(
                "No database backend configured. "
                "Add a 'databricks:' section (cloud warehouse) or a 'duckdb:' section "
                "(local file) to config.yaml. See README.md for examples."
            )
        if has_databricks and has_duckdb:
            raise ValueError(
                "Both 'databricks' and 'duckdb' sections are present in config.yaml. "
                "Configure exactly one backend."
            )
        return self

    @property
    def backend(self) -> str:
        """Active backend: 'databricks' or 'duckdb'."""
        return "duckdb" if self.duckdb is not None else "databricks"

    @property
    def db_schema(self) -> str:
        """Schema name for the active backend."""
        if self.duckdb is not None:
            return self.duckdb.schema
        return self.databricks.schema  # type: ignore[union-attr]

    @property
    def sandbox_prefix(self) -> str:
        """Sandbox table prefix for the active backend."""
        if self.duckdb is not None:
            return self.duckdb.sandbox_prefix
        return self.databricks.sandbox_prefix  # type: ignore[union-attr]

    @property
    def audit_table(self) -> str:
        """Audit table name for the active backend."""
        if self.duckdb is not None:
            return self.duckdb.audit_table
        return self.databricks.audit_table  # type: ignore[union-attr]


_settings: Settings | None = None


def reset_settings() -> None:
    """Clear the cached settings so the next get_settings() call re-reads config.yaml.
    Call this after programmatically updating config.yaml (e.g. from configure_* tools).
    """
    global _settings
    _settings = None


def get_settings() -> Settings:
    global _settings
    if _settings is not None:
        return _settings

    config_path = _find_config_path()
    if not config_path.exists():
        env_hint = (
            f"\n   (looked at PRELIGHT_CONFIG={config_path})"
            if os.environ.get("PRELIGHT_CONFIG")
            else f"\n   (looked for config.yaml in {Path.cwd()})"
        )
        print(
            f"❌ config.yaml not found.{env_hint}\n"
            "   Run setup.sh to create one, or tell Claude 'Configure DuckDB' / 'Configure Databricks'.\n"
            "   Then set PRELIGHT_CONFIG=/absolute/path/to/config.yaml in Claude Desktop."
        )
        sys.exit(1)

    try:
        raw = yaml.safe_load(config_path.read_text())
    except yaml.YAMLError as e:
        print(f"❌ config.yaml is not valid YAML:\n   {e}")
        sys.exit(1)

    if not raw:
        print(
            "❌ config.yaml is empty. Add a 'databricks:' or 'duckdb:' section — see README.md."
        )
        sys.exit(1)

    try:
        _settings = Settings(**raw)
    except ValidationError as e:
        lines = []
        for err in e.errors():
            loc = " -> ".join(str(p) for p in err["loc"])
            lines.append(f"   {loc}: {err['msg']}")
        print("❌ config.yaml validation failed:\n" + "\n".join(lines))
        sys.exit(1)

    return _settings
