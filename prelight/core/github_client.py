from __future__ import annotations

import re
from datetime import datetime, timezone

from github import Github, GithubException

from prelight.config.settings import get_settings
from prelight.core import context_generator


# ── Helpers ───────────────────────────────────────────────────────────────────


def _slug(text: str, max_len: int = 40) -> str:
    """'Cancel all pending orders' → 'cancel-all-pending-orders'"""
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")[:max_len]


def _migration_path(folder: str, ts: str, source_table: str, description: str) -> str:
    """Build the new per-PR file path: migrations/2026-04-02-143022-orders-cancel-pending.sql"""
    return f"{folder}/{ts}-{source_table}-{_slug(description)}.sql"


def _rewrite_to_production(sql: str, schema: str, sandbox_name: str, source_table: str) -> str:
    sql = re.sub(
        rf"\b{re.escape(schema)}\.{re.escape(sandbox_name)}\b",
        f"{schema}.{source_table}",
        sql,
        flags=re.IGNORECASE,
    )
    return re.sub(
        rf"\b{re.escape(sandbox_name)}\b",
        source_table,
        sql,
        flags=re.IGNORECASE,
    )


def _build_migration_block(
    schema: str,
    source_table: str,
    sandbox_name: str,
    production_sqls: list[str],
    quality_run_id: str | None,
    description: str,
    iso_ts: str,
) -> str:
    sql_body = "\n".join(
        s if s.rstrip().endswith(";") else s + ";" for s in production_sqls
    )
    return (
        f"-- Migration: {iso_ts}\n"
        f"-- Table:     {schema}.{source_table}\n"
        f"-- Sandbox:   {sandbox_name}\n"
        f"-- Quality:   {quality_run_id or 'not run'}\n"
        f"-- {description}\n\n"
        f"{sql_body}\n"
    )


def _build_quality_section(
    quality_check_results: list[dict],
    quality_run_id: str | None,
) -> tuple[str, str]:
    """Returns (markdown_section, status_line)."""
    if not quality_check_results or not quality_run_id:
        return "", "not run"

    all_passed = all(c["status"] == "PASS" for c in quality_check_results)
    status_line = "✅ all checks passed" if all_passed else "❌ some checks failed"

    blocks = []
    for check in quality_check_results:
        icon = "✅" if check["status"] == "PASS" else "❌"
        desc = f"*{check['expected']}*\n\n" if check.get("expected") else ""
        block = f"#### {icon} {check['check']} — {check['status']}\n\n{desc}```sql\n{check['sql']}\n```"
        if check.get("result"):
            block += f"\n\n> **Result:** {check['result']}"
        if check.get("detail"):
            block += f"\n\n> **Failure detail:** {check['detail']}"
        blocks.append(block)

    section = (
        f"\n### Quality Checks\n\n"
        f"> Quality Run ID: `{quality_run_id}`\n\n"
        + "\n\n".join(blocks)
        + "\n"
    )
    return section, status_line


def _get_repo():
    gh = get_settings().github
    if gh is None:
        raise RuntimeError(
            "❌ GitHub is not configured. Use the configure_github tool to add your credentials."
        )
    try:
        return Github(gh.token).get_repo(gh.repo)
    except GithubException as e:
        raise RuntimeError(
            f"❌ GitHub: could not access repo '{gh.repo}': {e.data.get('message', e)}"
        ) from e


def _create_branch(repo, branch_name: str, base_branch: str) -> None:
    try:
        sha = repo.get_branch(base_branch).commit.sha
        repo.create_git_ref(ref=f"refs/heads/{branch_name}", sha=sha)
    except GithubException as e:
        raise RuntimeError(
            f"❌ GitHub: could not create branch '{branch_name}': {e.data.get('message', e)}"
        ) from e


def _commit_file(repo, path: str, message: str, content: str, branch: str) -> None:
    """Create a new file on the branch. Silently skips if creation fails."""
    try:
        repo.create_file(path=path, message=message, content=content, branch=branch)
    except GithubException:
        pass


def _commit_context_md(repo, branch: str, source_table: str, content: str) -> None:
    """Create or update context/{table}.md on the branch. Never blocks the PR."""
    path = f"context/{source_table}.md"
    try:
        try:
            existing = repo.get_contents(path, ref=branch)
            repo.update_file(path=path, message=f"context: {source_table}", content=content,
                             sha=existing.sha, branch=branch)
        except GithubException:
            repo.create_file(path=path, message=f"context: {source_table}", content=content, branch=branch)
    except GithubException:
        pass


def _read_existing_context(repo, source_table: str, base_branch: str) -> str | None:
    try:
        f = repo.get_contents(f"context/{source_table}.md", ref=base_branch)
        return f.decoded_content.decode("utf-8")
    except GithubException:
        return None


# ── Public API ────────────────────────────────────────────────────────────────


def raise_migration_pr(
    source_table: str,
    sandbox_name: str,
    applied_sqls: list[str],
    quality_run_id: str | None,
    description: str,
    quality_check_results: list[dict] | None = None,
    custom_quality_checks: list[dict] | None = None,
    schema_columns: list[dict] | None = None,
    context_notes: str | None = None,
) -> str:
    settings = get_settings()
    gh = settings.github  # non-None guaranteed by caller
    schema = settings.db_schema

    now = datetime.now(timezone.utc)
    iso_ts = now.isoformat()
    file_ts = now.strftime("%Y-%m-%d-%H%M%S")
    branch_ts = now.strftime("%Y%m%d-%H%M")

    production_sqls = [
        _rewrite_to_production(s, schema, sandbox_name, source_table)
        for s in applied_sqls
    ]

    migration_content = _build_migration_block(
        schema=schema,
        source_table=source_table,
        sandbox_name=sandbox_name,
        production_sqls=production_sqls,
        quality_run_id=quality_run_id,
        description=description,
        iso_ts=iso_ts,
    )

    repo = _get_repo()
    branch_name = f"migration/{source_table}-{branch_ts}"
    _create_branch(repo, branch_name, gh.base_branch)

    # Create the migration SQL as a new file (never appends to an existing file)
    migration_path = _migration_path(gh.migrations_folder, file_ts, source_table, description)
    try:
        repo.create_file(
            path=migration_path,
            message=f"migration: {source_table} — {description}",
            content=migration_content,
            branch=branch_name,
        )
    except GithubException as e:
        raise RuntimeError(
            f"❌ GitHub: could not commit migration file: {e.data.get('message', e)}"
        ) from e

    # Quality check SQL files
    if custom_quality_checks:
        for check in custom_quality_checks:
            name = check.get("name", "check")
            _commit_file(
                repo,
                path=f"quality_checks/runs/{sandbox_name}/{name}.sql",
                message=f"quality check: {name}",
                content=f"-- {check.get('description', name)}\n-- Sandbox: {sandbox_name}\n\n{check.get('sql', '')}\n",
                branch=branch_name,
            )

    # context/{table}.md
    existing_context = _read_existing_context(repo, source_table, gh.base_branch)
    _commit_context_md(
        repo, branch_name, source_table,
        context_generator.build_context_md(
            source_table=source_table,
            schema=schema,
            schema_columns=schema_columns or [],
            description=description,
            iso_ts=iso_ts,
            context_notes=context_notes,
            existing_content=existing_context,
        ),
    )

    quality_section, quality_status = _build_quality_section(quality_check_results or [], quality_run_id)
    quality_line = f"`{quality_run_id}` {quality_status}" if quality_run_id else "not run"
    sql_list = "\n".join(f"  {i+1}. `{s[:120]}`" for i, s in enumerate(production_sqls))

    pr_body = (
        f"## `{schema}.{source_table}`\n\n"
        f"**Sandbox:** `{sandbox_name}`  \n"
        f"**Quality:** {quality_line}  \n"
        f"**File:** `{migration_path}`\n\n"
        f"### SQL\n\n{sql_list}\n"
        f"{quality_section}\n---\n*Generated by prelight at {iso_ts}*"
    )

    try:
        pr = repo.create_pull(
            title=f"[Migration] {source_table} — {description}",
            body=pr_body,
            head=branch_name,
            base=gh.base_branch,
        )
    except GithubException as e:
        raise RuntimeError(
            f"❌ GitHub: could not open PR: {e.data.get('message', e)}"
        ) from e

    return pr.html_url


