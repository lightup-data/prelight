from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

from prelight.config.settings import get_settings
from prelight.core.clients import get_client


@dataclass
class SandboxRecord:
    sandbox_name: str
    source_table: str
    created_at: str
    applied_sqls: list[str] = field(default_factory=list)
    quality_passed: bool = False
    quality_run_id: str | None = None
    quality_check_results: list[dict] = field(default_factory=list)
    custom_quality_checks: list[dict] = field(default_factory=list)
    schema_columns: list[dict] = field(default_factory=list)
    migration_file_path: str | None = None


_registry: dict[str, SandboxRecord] = {}


def create_sandbox(source_table: str) -> SandboxRecord:
    client = get_client()
    settings = get_settings()
    schema = settings.db_schema
    prefix = settings.sandbox_prefix

    now = datetime.now(timezone.utc)
    timestamp = now.strftime("%Y%m%d_%H%M")
    sandbox_name = f"{prefix}{source_table}_{timestamp}"
    created_at = now.isoformat()

    source_full = f"{schema}.{source_table}"
    sandbox_full = f"{schema}.{sandbox_name}"
    client.create_sandbox_table(source_full, sandbox_full)

    record = SandboxRecord(
        sandbox_name=sandbox_name,
        source_table=source_table,
        created_at=created_at,
        schema_columns=client.get_table_schema(source_table),
    )
    _registry[sandbox_name] = record
    return record


def get_sandbox(sandbox_name: str) -> SandboxRecord:
    record = _registry.get(sandbox_name)
    if record is None:
        raise ValueError(
            f"❌ No sandbox found named '{sandbox_name}'. Use create_sandbox first."
        )
    return record


def get_sandbox_for_table(source_table: str) -> SandboxRecord | None:
    matches = [
        r for r in _registry.values() if r.source_table == source_table
    ]
    if not matches:
        return None
    return max(matches, key=lambda r: r.created_at)


def log_transformation(sandbox_name: str, sql: str) -> None:
    record = get_sandbox(sandbox_name)
    record.applied_sqls.append(sql)


def store_quality_results(sandbox_name: str, run_id: str, checks: list[dict]) -> None:
    record = get_sandbox(sandbox_name)
    record.quality_run_id = run_id
    record.quality_check_results = checks


def mark_quality_passed(sandbox_name: str) -> None:
    record = get_sandbox(sandbox_name)
    record.quality_passed = True


def list_sandboxes() -> list[SandboxRecord]:
    return list(_registry.values())
