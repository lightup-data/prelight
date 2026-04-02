from __future__ import annotations

import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from mcp.server.fastmcp import FastMCP

from prelight.config.settings import get_settings
from prelight.core import (
    context_generator,
    production_guard,
    quality_checks,
    sandbox_manager,
    sql_utils,
)
from prelight.core.clients import get_client

mcp = FastMCP(
    "prelight",
    host=os.environ.get("MCP_HOST", "127.0.0.1"),
    port=int(os.environ.get("MCP_PORT", "8000")),
)

# ── Session state ──────────────────────────────────────────────────────────────

_session_branch: str | None = None
_repo_root: str | None = None

# ── Helpers ───────────────────────────────────────────────────────────────────


def _run_git(*args: str, cwd: str) -> tuple[bool, str]:
    """Run a git command in cwd. Returns (success, stdout)."""
    result = subprocess.run(
        ["git", "-C", cwd] + list(args),
        capture_output=True,
        text=True,
    )
    return result.returncode == 0, result.stdout.strip()


def _detect_base_branch(repo_root: str) -> str | None:
    """Detect the base branch to cut migration branches from.

    Try in order:
      1. origin/HEAD — most reliable when a remote exists
      2. local 'main' branch exists
      3. local 'master' branch exists
      4. None — brand new repo with no commits yet (git init, no commits)
    """
    # Option 1: what origin considers its default branch
    ok, ref = _run_git("symbolic-ref", "--short", "refs/remotes/origin/HEAD", cwd=repo_root)
    if ok and ref:
        return ref.split("/")[-1]  # "origin/main" → "main"

    # Option 2: local main
    ok, _ = _run_git("show-ref", "--verify", "refs/heads/main", cwd=repo_root)
    if ok:
        return "main"

    # Option 3: local master
    ok, _ = _run_git("show-ref", "--verify", "refs/heads/master", cwd=repo_root)
    if ok:
        return "master"

    # Option 4: brand new repo — no commits yet, nothing to checkout from
    return None


def _fmt_rows(rows: list[dict], limit: int = 50) -> str:
    if not rows:
        return "(no rows returned)"
    rows = rows[:limit]
    headers = list(rows[0].keys())
    col_widths = [
        max(len(h), max((len(str(r.get(h, ""))) for r in rows), default=0))
        for h in headers
    ]
    sep = "-+-".join("-" * w for w in col_widths)
    header_line = " | ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers))
    data_lines = [
        " | ".join(str(r.get(h, "")).ljust(col_widths[i]) for i, h in enumerate(headers))
        for r in rows
    ]
    return "\n".join([header_line, sep] + data_lines)


def _fmt_quality_report(result: dict) -> str:
    lines = [
        f"Quality Check Report",
        f"Run ID:      {result['run_id']}",
        f"Sandbox:     {result['sandbox_name']}",
        f"Source:      {result['source_table']}",
        "",
    ]
    for check in result["checks"]:
        icon = "✅" if check["status"] == "PASS" else "❌"
        lines.append(f"{icon} {check['check']}: {check['status']}")
        lines.append(f"   Result:   {check['result']}")
        lines.append(f"   Expected: {check['expected']}")
        if check.get("detail"):
            lines.append(f"   Detail:   {check['detail']}")
        lines.append("")
    overall = "✅ ALL CHECKS PASSED" if result["all_passed"] else "❌ SOME CHECKS FAILED"
    lines.append(overall)
    return "\n".join(lines)


# ── Tool 1: start_migration ───────────────────────────────────────────────────


@mcp.tool()
def start_migration(
    description: str,
    working_directory: str,
    init_git: bool = False,
) -> str:
    """Start a new migration. ALWAYS call this before create_sandbox for every new migration
    request. It checks that working_directory is a git repo, creates a migration branch, and
    sets up local file tracking for all changes made in this session.

    description: what this migration does — used as the branch name
      (e.g. 'apply discount and mark premium customers')
    working_directory: the absolute path to the user's project directory. Pass this from
      your session context — the directory where Claude Code was opened.
    init_git: set to True only when the user has explicitly confirmed they want git init
      run in working_directory.
    """
    global _session_branch, _repo_root

    # Check git binary is available
    git_check = subprocess.run(["git", "--version"], capture_output=True)
    if git_check.returncode != 0:
        return "❌ git is not installed. Install git from https://git-scm.com and try again."

    # Optionally initialise a new repo
    if init_git:
        ok, out = _run_git("init", cwd=working_directory)
        if not ok:
            return f"❌ git init failed: {out}"
        # Create an empty initial commit so checkout -b has a base
        subprocess.run(
            ["git", "-C", working_directory, "commit", "--allow-empty", "-m", "init"],
            capture_output=True,
        )

    # Verify working_directory is a git repo
    ok, repo_root = _run_git("rev-parse", "--show-toplevel", cwd=working_directory)
    if not ok:
        return (
            f"⚠️  `{working_directory}` is not a git repo.\n\n"
            f"Prelight writes migration files directly into your repo as you work. "
            f"Should I run `git init` here to set one up? "
            f"Say **yes** and I'll call start_migration again with init_git=True."
        )

    # Stash uncommitted changes if the working tree is dirty
    ok, status_out = _run_git("status", "--porcelain", cwd=repo_root)
    stash_msg = ""
    if status_out:
        _run_git("stash", cwd=repo_root)
        stash_msg = "\n⚠️  Stashed your uncommitted changes before creating the migration branch."

    # Always cut the new branch from the base branch so each migration is independent
    base_branch = _detect_base_branch(repo_root)
    if base_branch:
        _run_git("checkout", base_branch, cwd=repo_root)

    # Create migration branch
    now = datetime.now(timezone.utc)
    branch_name = f"migration/{sql_utils.slug(description)}-{now.strftime('%Y%m%d-%H%M')}"
    ok, err = _run_git("checkout", "-b", branch_name, cwd=repo_root)
    if not ok:
        return f"❌ Failed to create branch '{branch_name}': {err}"

    _session_branch = branch_name
    _repo_root = repo_root

    return (
        f"✅ Migration started on branch `{branch_name}`."
        f"{stash_msg}\n\n"
        f"Ready — call create_sandbox to begin."
    )


# ── Tool 2: list_tables ───────────────────────────────────────────────────────


@mcp.tool()
def list_tables() -> str:
    """List all production tables in the configured schema. Excludes sandbox tables (sbx_ prefix)
    and the audit table. Use this to show the engineer what tables are available."""
    try:
        settings = get_settings()
        client = get_client()
        schema = settings.db_schema
        prefix = settings.sandbox_prefix
        audit = settings.audit_table

        table_names = client.list_table_names(schema)
        prod_tables = [
            t for t in table_names
            if not t.startswith(prefix) and t != audit and t
        ]
        if not prod_tables:
            return f"No production tables found in schema '{schema}'."
        table_list = "\n".join(f"  • {t}" for t in sorted(prod_tables))
        return f"Production tables in '{schema}':\n{table_list}"
    except RuntimeError as e:
        return str(e)
    except Exception as e:
        return f"❌ Failed to list tables: {e}"


# ── Tool 3: describe_table ────────────────────────────────────────────────────


@mcp.tool()
def describe_table(table: str) -> str:
    """Get the schema and row count of a production table. Shows column names, types, and
    whether nullable. Use before creating a sandbox or when the engineer wants to understand
    a table."""
    try:
        client = get_client()
        schema_cols = client.get_table_schema(table)
        row_count = client.get_row_count(table)

        if not schema_cols:
            return f"❌ Table '{table}' not found or has no columns."

        col_lines = "\n".join(
            f"  {c['column_name']:<30} {c['data_type']}" for c in schema_cols
        )
        return (
            f"Table: {table}\n"
            f"Row count: {row_count:,}\n\n"
            f"Columns:\n{col_lines}"
        )
    except RuntimeError as e:
        return str(e)
    except Exception as e:
        return f"❌ Failed to describe table '{table}': {e}"


# ── Tool 4: query_table ───────────────────────────────────────────────────────


@mcp.tool()
def query_table(sql: str) -> str:
    """Run a read-only SELECT query on a production table. Only SELECT statements allowed —
    any write attempt is hard-blocked. Use for the engineer to inspect production data."""
    try:
        production_guard.check_select_only(sql)
        client = get_client()
        rows = client.execute_query(sql)
        result = _fmt_rows(rows, limit=50)
        suffix = f"\n\n(showing first 50 rows)" if len(rows) >= 50 else ""
        return result + suffix
    except production_guard.ProductionWriteBlockedError as e:
        return str(e)
    except RuntimeError as e:
        return str(e)
    except Exception as e:
        return f"❌ Query failed: {e}"


# ── Tool 5: create_sandbox ────────────────────────────────────────────────────


@mcp.tool()
def create_sandbox(table: str) -> str:
    """Create a sandbox copy of a production table. The sandbox is a full data copy in the
    same schema with an sbx_ prefix. IMPORTANT: Always call this automatically before
    apply_transformation if the engineer hasn't explicitly created a sandbox for the target
    table. Never ask the engineer to do this manually — just do it.
    ALWAYS include the exact identity/guard message from the tool result verbatim in your
    response to the user — do not paraphrase or omit it. It starts with 🔒 and must be
    shown explicitly so the user knows production is protected."""
    if _session_branch is None:
        return (
            "⚠️  No active migration branch. "
            "Call start_migration(description, working_directory) first."
        )
    try:
        settings = get_settings()
        client = get_client()
        record = sandbox_manager.create_sandbox(table)
        row_count = client.get_row_count(record.sandbox_name)
        if settings.backend == "databricks" and settings.databricks.dual_token_mode:
            identity_msg = (
                f"🔒 Identity switched to sandbox writer — "
                f"production tables are now read-only by Databricks credential"
            )
        else:
            identity_msg = (
                f"🔒 Production guard active — "
                f"writes to production tables are hard-blocked by SQL inspection"
            )
        return (
            f"✅ Sandbox created: {record.sandbox_name} "
            f"(copied from {table}, {row_count:,} rows)\n"
            f"{identity_msg}"
        )
    except RuntimeError as e:
        return str(e)
    except Exception as e:
        return f"❌ Failed to create sandbox for '{table}': {e}"


# ── Tool 6: apply_transformation ─────────────────────────────────────────────


@mcp.tool()
def apply_transformation(sandbox_name: str, sql: str) -> str:
    """Apply a SQL transformation to a sandbox table. CRITICAL RULES: (1) Always call
    create_sandbox first if no sandbox exists for the target table — do this automatically
    without asking the engineer. (2) Rewrite the SQL to target the sandbox table name, not
    the production table name. (3) This runs production_guard before execution — any attempt
    to write to a non-sandbox table raises a hard error. Logs the transformation for later
    PR generation. (4) ALWAYS include the exact 🔒 guard message from the tool result
    verbatim in your response — never paraphrase or drop it. The user must see explicitly
    that production is protected on every transformation."""
    try:
        settings = get_settings()
        client = get_client()
        prefix = settings.sandbox_prefix
        audit = settings.audit_table

        production_guard.check_sql(sql, prefix, audit)
        record = sandbox_manager.get_sandbox(sandbox_name)
        client.execute_statement(sql)
        sandbox_manager.log_transformation(sandbox_name, sql)

        # Write migration SQL to local file and commit
        if _repo_root and _session_branch:
            schema = settings.db_schema
            prod_sql = sql_utils.rewrite_to_production(sql, schema, sandbox_name, record.source_table)
            sql_line = prod_sql if prod_sql.rstrip().endswith(";") else prod_sql + ";"
            migrations_dir = Path(_repo_root) / "migrations"
            migrations_dir.mkdir(exist_ok=True)

            if record.migration_file_path:
                with open(record.migration_file_path, "a") as f:
                    f.write(f"\n{sql_line}\n")
                n = len(record.applied_sqls)
                commit_msg = f"migration({record.source_table}): add transformation #{n}"
            else:
                now = datetime.now(timezone.utc)
                file_ts = now.strftime("%Y-%m-%d-%H%M%S")
                migration_path = migrations_dir / f"{file_ts}-{record.source_table}.sql"
                content = (
                    f"-- Migration: {now.isoformat()}\n"
                    f"-- Table:     {schema}.{record.source_table}\n"
                    f"-- Sandbox:   {sandbox_name}\n"
                    f"-- Branch:    {_session_branch}\n\n"
                    f"{sql_line}\n"
                )
                migration_path.write_text(content)
                record.migration_file_path = str(migration_path)
                commit_msg = f"migration({record.source_table}): add transformation SQL"

            _run_git("add", "migrations/", cwd=_repo_root)
            _run_git("commit", "-m", commit_msg, cwd=_repo_root)

        if settings.backend == "databricks" and settings.databricks.dual_token_mode:
            guard_msg = (
                "🔒 Sandbox identity used — production tables are write-protected by credential"
            )
        else:
            guard_msg = (
                "🔒 Production guard verified — SQL targets sandbox only, production tables untouched"
            )
        return (
            f"✅ Transformation applied to sandbox '{sandbox_name}'.\n"
            f"{guard_msg}\n"
            f"SQL logged for PR generation.\n\n"
            f"Next: read the quality_checks/ library folder, examine this transformation and the "
            f"table schema, pick the relevant checks, generate the comparison SQLs with real table "
            f"and column names filled in, then call save_quality_checks. After that the engineer "
            f"can call run_quality_checks.\n\n"
            f"IMPORTANT: every check SQL must use either (a) a 'status' column returning 'PASS'/'FAIL' "
            f"(as all library templates do), or (b) violation-row style returning 0 rows=PASS. "
            f"Bare COUNT(*) queries with no status column always fail evaluation."
        )
    except production_guard.ProductionWriteBlockedError as e:
        return str(e)
    except ValueError as e:
        return str(e)
    except RuntimeError as e:
        return str(e)
    except Exception as e:
        return f"❌ Transformation failed: {e}"


# ── Tool 7: preview_transformation ───────────────────────────────────────────


@mcp.tool()
def preview_transformation(sql: str) -> str:
    """Preview what a SQL query returns against the sandbox table, without persisting any
    changes. Use to let the engineer verify results before running quality checks."""
    try:
        settings = get_settings()
        production_guard.check_sql(sql, settings.sandbox_prefix, settings.audit_table)
        client = get_client()
        rows = client.execute_query(sql)
        result = _fmt_rows(rows, limit=50)
        suffix = "\n\n(showing first 50 rows)" if len(rows) >= 50 else ""
        return result + suffix
    except production_guard.ProductionWriteBlockedError as e:
        return str(e)
    except RuntimeError as e:
        return str(e)
    except Exception as e:
        return f"❌ Preview failed: {e}"


# ── Tool 8: save_quality_checks ───────────────────────────────────────────────


@mcp.tool()
def save_quality_checks(sandbox_name: str, checks: list[dict]) -> str:
    """Save the quality checks Claude has selected for a sandbox before running them.
    Must be called after apply_transformation and before run_quality_checks.

    Claude should: read the quality_checks/ library folder, examine the transformation SQL
    and table schema, pick relevant checks from the library and generate any bespoke checks
    specific to this transformation, then call this tool with the final list.

    Each check must be a dict with:
      name:        str  — unique identifier, used as the SQL filename
      sql:         str  — the comparison SQL (prod vs sandbox) with real table/column names
      description: str  — what this check verifies and what passing means

    CRITICAL — SQL must follow one of these two patterns or it will always be marked FAIL:

    Pattern 1 — status column (preferred, use for comparisons):
      Return a single row with a 'status' column containing 'PASS' or 'FAIL'.
      Example: SELECT ..., CASE WHEN <condition> THEN 'PASS' ELSE 'FAIL' END AS status FROM ...
      The library templates in quality_checks/ all use this pattern.

    Pattern 2 — violation rows (use for constraint checks):
      Return 0 rows = PASS, any rows = FAIL (each row is a violation).
      Example: SELECT * FROM sandbox WHERE null_col IS NULL  -- returns offending rows

    DO NOT write bare COUNT(*) queries with no status column — they always return 1 row
    and will always be evaluated as FAIL regardless of the count value.
    """
    try:
        record = sandbox_manager.get_sandbox(sandbox_name)
        record.custom_quality_checks = checks

        # Write check SQL files to local repo and commit
        if _repo_root and _session_branch:
            checks_dir = Path(_repo_root) / "quality_checks" / "runs" / sandbox_name
            checks_dir.mkdir(parents=True, exist_ok=True)
            for check in checks:
                name = check.get("name", "check")
                check_sql = check.get("sql", "")
                description = check.get("description", name)
                check_file = checks_dir / f"{name}.sql"
                check_file.write_text(
                    f"-- {description}\n-- Sandbox: {sandbox_name}\n\n{check_sql}\n"
                )
            _run_git("add", "quality_checks/", cwd=_repo_root)
            n = len(checks)
            _run_git(
                "commit", "-m",
                f"quality_checks({record.source_table}): save {n} check definition{'s' if n != 1 else ''}",
                cwd=_repo_root,
            )

        names = [c.get("name", "?") for c in checks]
        return (
            f"✅ {len(checks)} quality check(s) saved for sandbox '{sandbox_name}':\n"
            + "\n".join(f"  • {n}" for n in names)
            + "\n\nCall run_quality_checks to execute them."
        )
    except ValueError as e:
        return str(e)
    except Exception as e:
        return f"❌ Failed to save quality checks: {e}"


# ── Tool 9: run_quality_checks ────────────────────────────────────────────────


@mcp.tool()
def run_quality_checks(sandbox_name: str) -> str:
    """Run the quality checks Claude saved via save_quality_checks for this sandbox.
    Results are stored on the sandbox record and written to MIGRATION_NOTES.md locally.
    Quality checks are advisory — the PR can be raised regardless of pass/fail."""
    try:
        record = sandbox_manager.get_sandbox(sandbox_name)

        if not record.custom_quality_checks:
            return (
                f"❌ No quality checks defined for sandbox '{sandbox_name}'. "
                f"Read the quality_checks/ library, pick relevant checks for this transformation, "
                f"and call save_quality_checks first."
            )

        result = quality_checks.run_custom_checks(
            sandbox_name, record.source_table, record.custom_quality_checks
        )

        sandbox_manager.store_quality_results(sandbox_name, result["run_id"], result["checks"])
        if result["all_passed"]:
            sandbox_manager.mark_quality_passed(sandbox_name)

        # Write context/{table}.md and MIGRATION_NOTES.md to local repo
        if _repo_root and _session_branch:
            settings = get_settings()
            schema = settings.db_schema
            now = datetime.now(timezone.utc)
            iso_ts = now.isoformat()

            # 1. context/{table}.md
            context_dir = Path(_repo_root) / "context"
            context_dir.mkdir(exist_ok=True)
            context_path = context_dir / f"{record.source_table}.md"
            existing_content = context_path.read_text() if context_path.exists() else None
            context_md = context_generator.build_context_md(
                source_table=record.source_table,
                schema=schema,
                schema_columns=record.schema_columns,
                description=record.source_table,
                iso_ts=iso_ts,
                existing_content=existing_content,
            )
            context_path.write_text(context_md)
            _run_git("add", "context/", cwd=_repo_root)
            _run_git(
                "commit", "-m",
                f"context({record.source_table}): update table context",
                cwd=_repo_root,
            )

            # 2. MIGRATION_NOTES.md — append table section (accumulates across tables)
            production_sqls = [
                sql_utils.rewrite_to_production(s, schema, sandbox_name, record.source_table)
                for s in record.applied_sqls
            ]
            sql_block = "\n\n".join(
                f"```sql\n{s if s.rstrip().endswith(';') else s + ';'}\n```"
                for s in production_sqls
            )
            check_lines: list[str] = []
            for check in result["checks"]:
                icon = "✅" if check["status"] == "PASS" else "❌"
                check_lines.append(f"**{icon} {check['check']} — {check['status']}**")
                check_lines.append(f"  - Result: {check['result']}")
                if check.get("detail"):
                    check_lines.append(f"  - Detail: {check['detail']}")

            overall = "✅ All checks passed" if result["all_passed"] else "❌ Some checks failed"
            table_section = (
                f"## `{schema}.{record.source_table}`\n\n"
                f"**Sandbox:** `{sandbox_name}`  \n"
                f"**Quality Run:** `{result['run_id']}` — {overall}  \n\n"
                f"### SQL Applied\n\n"
                f"{sql_block}\n\n"
                f"### Quality Checks\n\n"
                + "\n".join(check_lines) + "\n"
            )

            notes_path = Path(_repo_root) / "MIGRATION_NOTES.md"
            if notes_path.exists():
                with open(notes_path, "a") as f:
                    f.write(f"\n---\n\n{table_section}")
            else:
                notes_path.write_text(
                    f"# Migration Notes\n\n"
                    f"**Branch:** `{_session_branch}`  \n"
                    f"**Date:** {iso_ts}\n\n"
                    f"---\n\n"
                    f"{table_section}"
                )

            _run_git("add", "MIGRATION_NOTES.md", cwd=_repo_root)
            _run_git(
                "commit", "-m",
                f"notes({record.source_table}): add quality check results",
                cwd=_repo_root,
            )

        report = _fmt_quality_report(result)
        pr_nudge = (
            f"\n\nChanges committed to branch `{_session_branch}`. "
            f"Say **raise a PR** with a short description when you're ready to push."
        ) if _session_branch else ""

        return report + pr_nudge

    except ValueError as e:
        return str(e)
    except RuntimeError as e:
        return str(e)
    except Exception as e:
        return f"❌ Quality checks failed: {e}"


# ── Tool 10: raise_pr ─────────────────────────────────────────────────────────


@mcp.tool()
def raise_pr(description: str) -> str:
    """Push the current migration branch and provide a link to open a PR. Call this when
    the user says 'raise a PR' or similar. Use their exact words as the description.
    description: clear description of what this migration does — becomes the PR title
      (e.g. 'Apply 10% discount to orders over $500 and mark customers as premium')"""
    if not _session_branch or not _repo_root:
        return (
            "❌ No active migration branch. "
            "Call start_migration(description, working_directory) first."
        )

    # Check remote
    ok, remote_url = _run_git("remote", "get-url", "origin", cwd=_repo_root)
    if not ok or not remote_url:
        return (
            f"Your changes are committed to branch `{_session_branch}`.\n\n"
            f"This repo has no remote configured — add one and push when ready:\n"
            f"  git remote add origin <url>\n"
            f"  git push -u origin {_session_branch}"
        )

    # Push
    ok, output = _run_git("push", "-u", "origin", _session_branch, cwd=_repo_root)
    if not ok:
        return f"❌ Push failed: {output}"

    # Construct compare URL if GitHub remote
    pr_link = ""
    match = re.search(r"github\.com[:/](.+?)(?:\.git)?$", remote_url)
    if match:
        repo_path = match.group(1)
        encoded_title = description.replace(" ", "+")
        compare_url = (
            f"https://github.com/{repo_path}/compare/{_session_branch}"
            f"?expand=1&title=%5BMigration%5D+{encoded_title}"
        )
        pr_link = f"\n\nOpen your PR here:\n{compare_url}"

    return (
        f"✅ Branch `{_session_branch}` pushed.\n\n"
        f"PR title: **[Migration] {description}**"
        f"{pr_link}"
    )


# ── Tool 11: configure_duckdb ─────────────────────────────────────────────────


@mcp.tool()
def configure_duckdb(
    path: str = "",
    schema: str = "analytics",
) -> str:
    """Switch the active database backend to DuckDB and update config.yaml.

    Call this when the user says they want to use DuckDB, mentions a .duckdb file,
    or wants to switch away from Databricks.

    Before calling, ask the user:
      - "What is the path to your DuckDB file?" (optional — leave empty for the default
        ~/.prelight/prelight.duckdb, or if they just want a fresh local database)

    path:   Path to an existing .duckdb file, or empty to use the default location.
            Use an absolute path (e.g. /Users/alice/mydb.duckdb) or relative.
    schema: Schema name inside the DuckDB file (default: analytics).
            Ask the user only if they have an existing file with a different schema.

    After this tool runs, demo data is ready if using the default path.
    The user can then say 'List my tables' to see the available tables.
    """
    import os
    from prelight.cli.configure import apply_duckdb
    from prelight.core.clients import reset_all

    resolved_path = path.strip() or None

    # Default to ~/.prelight/prelight.duckdb when no path given
    if not resolved_path:
        resolved_path = str(os.path.expanduser("~/.prelight/prelight.duckdb"))

    config_file = apply_duckdb(
        path=resolved_path,
        schema=schema,
        sandbox_prefix="sbx_",
        audit_table="qg_quality_results",
    )
    reset_all()

    # Load demo data if the file is new / empty
    try:
        import duckdb as _duckdb
        conn = _duckdb.connect(resolved_path)
        tables = conn.execute(
            f"SELECT COUNT(*) AS n FROM information_schema.tables WHERE table_schema = '{schema}'"
        ).fetchone()
        conn.close()
        needs_demo = tables is None or tables[0] == 0
    except Exception:
        needs_demo = True

    demo_msg = ""
    if needs_demo:
        try:
            from prelight.cli.setup_demo import run_setup_demo_core
            demo_result = run_setup_demo_core()
            demo_msg = f"\n\nDemo data loaded:\n{demo_result}"
        except Exception as e:
            demo_msg = f"\n\n⚠️  Could not load demo data: {e}"

    return (
        f"✅ Switched to DuckDB backend.\n"
        f"   File:   {resolved_path}\n"
        f"   Schema: {schema}\n"
        f"   Config: {config_file}"
        f"{demo_msg}\n\n"
        f"You can now say: List my tables"
    )


# ── Tool 12: configure_databricks ─────────────────────────────────────────────


@mcp.tool()
def configure_databricks(
    host: str,
    http_path: str,
    token: str = "",
    prod_token: str = "",
    sandbox_token: str = "",
    schema: str = "analytics",
    catalog: str = "hive_metastore",
) -> str:
    """Switch the active database backend to Databricks and update config.yaml.

    Call this when the user says they want to use Databricks, or provides
    Databricks credentials. DO NOT call with placeholder values — collect
    real credentials from the user first.

    Before calling, ask the user for:
      1. Workspace URL  (e.g. https://adb-1234567890.azuredatabricks.net)
      2. SQL Warehouse HTTP path  (find it: Warehouse → Connection Details → HTTP Path)
      3. Access token — two options:
           • Single token (simpler): one PAT with read+write access → use `token`
           • Dual tokens (more secure): read-only PAT → `prod_token`,
             write-only PAT scoped to sbx_* tables → `sandbox_token`
      4. Schema name (default: analytics — ask only if they use a different one)

    host:          Databricks workspace URL (must start with https://)
    http_path:     SQL Warehouse HTTP path (/sql/1.0/warehouses/...)
    token:         Single PAT (use this OR prod_token+sandbox_token, not both)
    prod_token:    Read-only PAT for production tables (dual-token mode)
    sandbox_token: Write-enabled PAT for sandbox tables (dual-token mode)
    schema:        Databricks schema containing the tables
    catalog:       Databricks catalog (default: hive_metastore)
    """
    from prelight.cli.configure import apply_databricks
    from prelight.core.clients import reset_all

    host = host.strip()
    if not host.startswith("https://"):
        host = "https://" + host
    host = host.rstrip("/")

    use_dual = bool(prod_token.strip() and sandbox_token.strip())
    use_single = bool(token.strip())

    if not use_dual and not use_single:
        return (
            "❌ No Databricks token provided. Please supply either:\n"
            "  • token — a single PAT with read+write access, or\n"
            "  • prod_token + sandbox_token — two separate PATs (recommended)"
        )

    config_file = apply_databricks(
        host=host,
        http_path=http_path.strip(),
        schema=schema.strip(),
        token=token.strip() or None,
        prod_token=prod_token.strip() or None,
        sandbox_token=sandbox_token.strip() or None,
        sandbox_prefix="sbx_",
        audit_table="qg_quality_results",
        catalog=catalog.strip(),
    )
    reset_all()

    mode = "dual-token (recommended)" if use_dual else "single-token"
    return (
        f"✅ Switched to Databricks backend ({mode} mode).\n"
        f"   Host:   {host}\n"
        f"   Schema: {schema}\n"
        f"   Config: {config_file}\n\n"
        f"You can now say: List my tables\n"
        f"Or run demo setup: setup_demo"
    )


# ── Tool 13: setup_demo ───────────────────────────────────────────────────────


@mcp.tool()
def setup_demo() -> str:
    """Create the demo schema and tables in the configured database backend.
    For Databricks: runs setup/databricks/demo_data.sql.
    For DuckDB: runs setup/duckdb/demo_data.sql directly via the DuckDB connection.
    Safe to run multiple times — drops and recreates tables each time for a clean state."""
    try:
        from prelight.cli.setup_demo import run_setup_demo_core
        return run_setup_demo_core()
    except RuntimeError as e:
        return str(e)
    except Exception as e:
        return f"❌ Demo setup failed: {e}"


# ── MCP Prompts ───────────────────────────────────────────────────────────────


@mcp.prompt()
def set_up_demo_data_assets() -> str:
    """Prompt to set up demo data in the configured database for an end-to-end walkthrough."""
    return "Please set up the demo data assets by calling the setup_demo tool."


@mcp.prompt()
def walk_me_through_an_end_to_end_transformation() -> str:
    """Guided end-to-end demo: start migration → list tables → sandbox → transform →
    quality check → optional PR."""
    return (
        "Walk me through an end-to-end transformation using the Lightup demo data. "
        "Start by calling start_migration with a short description of what we'll do and "
        "the current working directory. Then list the available tables, pick one to transform, "
        "create a sandbox, apply a change, run quality checks, and suggest raising a PR. "
        "The PR step is optional — confirm with me before pushing."
    )


# ── Entry point ───────────────────────────────────────────────────────────────


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "install":
        from prelight.cli.install import run_install
        run_install()
        return

    if len(sys.argv) > 1 and sys.argv[1] == "setup-demo":
        from prelight.cli.setup_demo import run_setup_demo
        run_setup_demo()
        return

    get_settings()  # validate config on startup — will exit if invalid

    transport = os.environ.get("MCP_TRANSPORT", "stdio")
    port = int(os.environ.get("MCP_PORT", "8000"))
    host = os.environ.get("MCP_HOST", "127.0.0.1")

    if transport == "sse":
        print(f"Starting Lightup MCP server (SSE) on http://{host}:{port}/sse")
        mcp.run(transport="sse")
    else:
        mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
