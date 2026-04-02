from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

from prelight.config.settings import get_settings
from prelight.core.clients import get_client


@dataclass
class WorkspaceRecord:
    workspace_name: str
    sandbox_names: list[str]
    created_at: str
    workspace_quality_run_id: str | None = None
    all_quality_passed: bool = False


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


_registry: dict[str, SandboxRecord] = {}
_workspace_registry: dict[str, WorkspaceRecord] = {}


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


def create_workspace(workspace_name: str, sandbox_names: list[str]) -> WorkspaceRecord:
    if workspace_name in _workspace_registry:
        raise ValueError(
            f"❌ Workspace '{workspace_name}' already exists."
        )
    if len(sandbox_names) < 2:
        raise ValueError(
            f"❌ A workspace needs at least 2 sandboxes. "
            f"For a single sandbox use raise_pr directly."
        )
    if len(sandbox_names) != len(set(sandbox_names)):
        raise ValueError("❌ Duplicate sandbox names in workspace.")

    records = [get_sandbox(name) for name in sandbox_names]

    # each source table may only appear once per workspace
    source_tables = [r.source_table for r in records]
    if len(source_tables) != len(set(source_tables)):
        duplicates = [t for t in source_tables if source_tables.count(t) > 1]
        raise ValueError(
            f"❌ Multiple sandboxes from the same source table: {', '.join(set(duplicates))}. "
            f"Each source table may appear only once in a workspace."
        )

    record = WorkspaceRecord(
        workspace_name=workspace_name,
        sandbox_names=sandbox_names,
        created_at=datetime.now(timezone.utc).isoformat(),
    )
    _workspace_registry[workspace_name] = record
    return record


def get_workspace(workspace_name: str) -> WorkspaceRecord:
    record = _workspace_registry.get(workspace_name)
    if record is None:
        raise ValueError(
            f"❌ No workspace found named '{workspace_name}'. Use create_workspace first."
        )
    return record


def list_workspaces() -> list[WorkspaceRecord]:
    return list(_workspace_registry.values())


def mark_workspace_quality_passed(workspace_name: str, workspace_run_id: str) -> None:
    record = get_workspace(workspace_name)
    record.workspace_quality_run_id = workspace_run_id
    record.all_quality_passed = True


def mark_workspace_quality_failed(workspace_name: str, workspace_run_id: str) -> None:
    record = get_workspace(workspace_name)
    record.workspace_quality_run_id = workspace_run_id
    record.all_quality_passed = False
