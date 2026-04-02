from __future__ import annotations

import os
import sys

from mcp.server.fastmcp import FastMCP

from prelight.config.settings import get_settings
from prelight.core import (
    github_client,
    production_guard,
    quality_checks,
    sandbox_manager,
)
from prelight.core.clients import get_client

mcp = FastMCP(
    "prelight",
    host=os.environ.get("MCP_HOST", "127.0.0.1"),
    port=int(os.environ.get("MCP_PORT", "8000")),
)

# ── Helpers ───────────────────────────────────────────────────────────────────


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


# ── Tool 1: list_tables ───────────────────────────────────────────────────────


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


# ── Tool 2: describe_table ────────────────────────────────────────────────────


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


# ── Tool 3: query_table ───────────────────────────────────────────────────────


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


# ── Tool 4: create_sandbox ────────────────────────────────────────────────────


@mcp.tool()
def create_sandbox(table: str) -> str:
    """Create a sandbox copy of a production table. The sandbox is a full data copy in the
    same schema with an sbx_ prefix. IMPORTANT: Always call this automatically before
    apply_transformation if the engineer hasn't explicitly created a sandbox for the target
    table. Never ask the engineer to do this manually — just do it.
    ALWAYS include the exact identity/guard message from the tool result verbatim in your
    response to the user — do not paraphrase or omit it. It starts with 🔒 and must be
    shown explicitly so the user knows production is protected."""
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


# ── Tool 5: apply_transformation ─────────────────────────────────────────────


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

        # Verify sandbox exists in registry
        sandbox_manager.get_sandbox(sandbox_name)

        client.execute_statement(sql)
        sandbox_manager.log_transformation(sandbox_name, sql)

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


# ── Tool 6: preview_transformation ───────────────────────────────────────────


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


# ── Tool 7: save_quality_checks ───────────────────────────────────────────────


@mcp.tool()
def save_quality_checks(sandbox_name: str, checks: list[dict]) -> str:
    """Save the quality checks Claude has selected for a sandbox before running them.
    Must be called after apply_transformation and before run_quality_checks.

    Claude should: read the quality_checks/ library folder, examine the transformation SQL
    and table schema, pick relevant checks from the library and generate any bespoke checks
    specific to this transformation, then call this tool with the final list.

    Each check must be a dict with:
      name:        str  — unique identifier, used as the SQL filename in the PR branch
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


# ── Tool 8: run_quality_checks ────────────────────────────────────────────────


@mcp.tool()
def run_quality_checks(sandbox_name: str) -> str:
    """Run the quality checks Claude saved via save_quality_checks for this sandbox.
    Results are stored on the sandbox record and included in the PR description.
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

        return _fmt_quality_report(result)
    except ValueError as e:
        return str(e)
    except RuntimeError as e:
        return str(e)
    except Exception as e:
        return f"❌ Quality checks failed: {e}"


# ── Tool 9: raise_pr ─────────────────────────────────────────────────────────


@mcp.tool()
def raise_pr(sandbox_name: str, description: str, context_notes: str = "") -> str:
    """Raise a GitHub pull request with the migration SQL for all transformations applied to
    the sandbox. Quality checks are advisory — the PR can be raised regardless of whether
    checks passed or failed, and even if no checks were run. Check results are included in
    the PR description when available. Creates a new dated SQL file in the migrations folder
    and opens a PR to the configured base branch.
    The description is used as the PR title — keep it short and specific, 5-8 words
    (e.g. 'Cancel all pending orders', 'Apply 10% markup to completed orders').
    context_notes: 1-3 sentences describing what this table is for and what it contains —
    written as a statement, not a question. Use everything you know: the table name, columns,
    the transformation just applied, and the description. This becomes the Purpose section of
    context/{table}.md. Example: 'The orders table records every customer purchase. It tracks
    order status through its lifecycle from placement to fulfilment.'"""
    try:
        settings = get_settings()
        if not settings.github:
            return (
                "❌ GitHub is not configured — cannot raise a PR.\n"
                "Use the configure_github tool to add your GitHub token and repo."
            )

        record = sandbox_manager.get_sandbox(sandbox_name)

        if not record.applied_sqls:
            return (
                f"❌ No transformations have been applied to sandbox '{sandbox_name}'. "
                f"Use apply_transformation first."
            )

        pr_url = github_client.raise_migration_pr(
            source_table=record.source_table,
            sandbox_name=sandbox_name,
            applied_sqls=record.applied_sqls,
            quality_run_id=record.quality_run_id,
            description=description,
            quality_check_results=record.quality_check_results,
            custom_quality_checks=record.custom_quality_checks,
            schema_columns=record.schema_columns,
            context_notes=context_notes or None,
        )

        return (
            f"✅ PR opened successfully!\n"
            f"URL: {pr_url}\n\n"
            f"Summary:\n"
            f"  Source table:    {record.source_table}\n"
            f"  Sandbox:         {sandbox_name}\n"
            f"  Quality run ID:  {record.quality_run_id or 'not run'}\n"
            f"  Transformations: {len(record.applied_sqls)}"
        )
    except ValueError as e:
        return str(e)
    except RuntimeError as e:
        return str(e)
    except Exception as e:
        return f"❌ Failed to raise PR: {e}"


# ── Tool 10: create_workspace ──────────────────────────────────────────────────


@mcp.tool()
def create_workspace(workspace_name: str, sandbox_names: list[str]) -> str:
    """Group two or more existing sandboxes into a named workspace so they can be
    quality-checked and raised as a single PR together. Use this when a single request
    touches multiple tables. Each sandbox must already exist (created via create_sandbox).
    Each source table may appear only once in a workspace."""
    try:
        record = sandbox_manager.create_workspace(workspace_name, sandbox_names)
        lines = []
        for name in record.sandbox_names:
            sbx = sandbox_manager.get_sandbox(name)
            lines.append(f"  • {name}  →  {sbx.source_table}")
        return (
            f"✅ Workspace '{workspace_name}' created with {len(sandbox_names)} sandboxes:\n"
            + "\n".join(lines)
        )
    except ValueError as e:
        return str(e)
    except Exception as e:
        return f"❌ Failed to create workspace: {e}"


# ── Tool 11: run_workspace_quality_checks ─────────────────────────────────────


@mcp.tool()
def run_workspace_quality_checks(workspace_name: str) -> str:
    """Run quality checks across all sandboxes in a workspace in one go. Results are
    stored per-sandbox and included in the workspace PR description. Quality checks are
    advisory — the workspace PR can be raised regardless of pass/fail.

    Before calling this tool, Claude MUST call save_quality_checks for EACH sandbox in
    the workspace. The process for each sandbox is identical to the single-sandbox flow:
      1. Read the quality_checks/ library folder to find relevant templates
      2. Examine the transformation SQL and table schema for that sandbox
      3. Generate SQLs from library templates with real table/column names filled in
      4. Add any bespoke checks specific to that transformation
      5. Call save_quality_checks for that sandbox

    CRITICAL — every check SQL must follow one of these two patterns:
      Pattern 1 (status column, preferred): return a row with a 'status' column
        containing 'PASS' or 'FAIL'. All library templates use this pattern.
        Example: SELECT ..., CASE WHEN <cond> THEN 'PASS' ELSE 'FAIL' END AS status FROM ...
      Pattern 2 (violation rows): return 0 rows = PASS, any rows = FAIL.
        Example: SELECT * FROM sandbox WHERE key_col IS NULL

    DO NOT write bare COUNT(*) queries with no status column — they always return 1 row
    and will always be evaluated as FAIL regardless of the count value."""
    try:
        workspace = sandbox_manager.get_workspace(workspace_name)
        records = [sandbox_manager.get_sandbox(name) for name in workspace.sandbox_names]

        # Ensure each sandbox has checks defined before running
        missing = [r.sandbox_name for r in records if not r.custom_quality_checks]
        if missing:
            return (
                f"❌ The following sandboxes have no quality checks defined: "
                f"{', '.join(missing)}. Call save_quality_checks for each sandbox first."
            )

        result = quality_checks.run_custom_checks_for_workspace(workspace_name, records)

        # Update individual sandbox records
        for sbx_result in result["per_sandbox_results"]:
            sandbox_manager.store_quality_results(
                sbx_result["sandbox_name"],
                sbx_result["run_id"],
                sbx_result["checks"],
            )
            if sbx_result["all_passed"]:
                sandbox_manager.mark_quality_passed(sbx_result["sandbox_name"])

        if result["all_passed"]:
            sandbox_manager.mark_workspace_quality_passed(
                workspace_name, result["workspace_run_id"]
            )
        else:
            sandbox_manager.mark_workspace_quality_failed(
                workspace_name, result["workspace_run_id"]
            )

        return _fmt_workspace_quality_report(result)
    except ValueError as e:
        return str(e)
    except RuntimeError as e:
        return str(e)
    except Exception as e:
        return f"❌ Workspace quality checks failed: {e}"


# ── Tool 12: raise_workspace_pr ───────────────────────────────────────────────


@mcp.tool()
def raise_workspace_pr(
    workspace_name: str,
    description: str,
) -> str:
    """Raise a single GitHub PR covering all tables in the workspace. All sandboxes must
    have transformations applied. Quality checks are advisory — the PR can be raised
    regardless of pass/fail. Check results are included in the PR description when available."""
    try:
        settings = get_settings()
        if not settings.github:
            return (
                "❌ GitHub is not configured — cannot raise a PR.\n"
                "Use the configure_github tool to add your GitHub token and repo."
            )

        workspace = sandbox_manager.get_workspace(workspace_name)
        records = [sandbox_manager.get_sandbox(name) for name in workspace.sandbox_names]

        # Guard: every sandbox must have at least one transformation
        empty = [r.sandbox_name for r in records if not r.applied_sqls]
        if empty:
            return (
                f"❌ The following sandboxes have no transformations applied: "
                f"{', '.join(empty)}. Use apply_transformation first."
            )

        pr_url = github_client.raise_workspace_migration_pr(
            workspace_name=workspace_name,
            sandbox_records=records,
            workspace_quality_run_id=workspace.workspace_quality_run_id,
            description=description,
        )

        total = sum(len(r.applied_sqls) for r in records)
        table_lines = "\n".join(
            f"  • {r.source_table} ({len(r.applied_sqls)} transformation(s))"
            for r in records
        )
        return (
            f"✅ Workspace PR opened successfully!\n"
            f"URL: {pr_url}\n\n"
            f"Summary:\n"
            f"  Workspace:             {workspace_name}\n"
            f"  Workspace run ID:      {workspace.workspace_quality_run_id or 'not run'}\n"
            f"  Total transformations: {total}\n"
            f"  Tables:\n{table_lines}"
        )
    except ValueError as e:
        return str(e)
    except RuntimeError as e:
        return str(e)
    except Exception as e:
        return f"❌ Failed to raise workspace PR: {e}"


def _fmt_workspace_quality_report(result: dict) -> str:
    lines = [
        f"Workspace Quality Check Report",
        f"Workspace:        {result['workspace_name']}",
        f"Workspace Run ID: {result['workspace_run_id']}",
        "",
    ]
    passed_count = sum(1 for r in result["per_sandbox_results"] if r["all_passed"])
    total_count = len(result["per_sandbox_results"])

    for sbx_result in result["per_sandbox_results"]:
        lines.append(
            f"{'━' * 3} {sbx_result['source_table']} ({sbx_result['sandbox_name']}) {'━' * 3}"
        )
        for check in sbx_result["checks"]:
            icon = "✅" if check["status"] == "PASS" else "❌"
            lines.append(f"{icon} {check['check']}: {check['status']}")
            lines.append(f"   Result:   {check['result']}")
            lines.append(f"   Expected: {check['expected']}")
            if check.get("detail"):
                lines.append(f"   Detail:   {check['detail']}")
        overall = "✅ ALL CHECKS PASSED" if sbx_result["all_passed"] else "❌ SOME CHECKS FAILED"
        lines.append(overall)
        lines.append("")

    lines.append("═" * 50)
    if result["all_passed"]:
        lines.append(f"Overall: ✅ All {total_count} sandboxes passed quality checks")
    else:
        lines.append(
            f"Overall: ❌ {total_count - passed_count} of {total_count} sandboxes failed"
        )
    return "\n".join(lines)


# ── Tool 13: configure_duckdb ─────────────────────────────────────────────────


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


# ── Tool 14: configure_databricks ─────────────────────────────────────────────


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


# ── Tool 15: configure_github ─────────────────────────────────────────────────


@mcp.tool()
def configure_github(
    token: str,
    repo: str,
    migrations_folder: str = "migrations",
    base_branch: str = "main",
) -> str:
    """Add or update GitHub credentials in config.yaml.

    Call this when the user wants to enable PR raising, says they have a GitHub token,
    or when raise_pr fails because GitHub is not configured.

    Before calling, ask the user for:
      1. GitHub personal access token (PAT with 'repo' scope)
      2. Repository in owner/repo-name format (e.g. alice/data-platform)
      3. Migrations folder in the repo (default: migrations)
      4. Base branch for PRs (default: main)

    Each PR creates a new dated file inside migrations_folder — no pre-existing file required.
    """
    from prelight.cli.configure import apply_github
    from prelight.config.settings import reset_settings

    if "/" not in repo:
        return "❌ Repository must be in 'owner/repo-name' format (e.g. alice/data-platform)."

    config_file = apply_github(
        token=token.strip(),
        repo=repo.strip(),
        migrations_folder=migrations_folder.strip(),
        base_branch=base_branch.strip(),
    )
    reset_settings()

    return (
        f"✅ GitHub configured.\n"
        f"   Repo:             {repo}\n"
        f"   Migrations folder: {migrations_folder}\n"
        f"   Branch:           {base_branch}\n"
        f"   Config:           {config_file}\n\n"
        f"You can now use raise_pr to open pull requests."
    )


# ── Tool 16: setup_demo ───────────────────────────────────────────────────────


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
    """Guided end-to-end demo: list tables → sandbox → transform → quality check → PR."""
    return (
        "Walk me through an end-to-end transformation using the Lightup demo data. "
        "Start by listing the available tables, then pick one to transform, create a sandbox, "
        "apply a change, run quality checks, and raise a PR."
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
        # Standalone mode: server runs independently, Claude Desktop connects via HTTP.
        # Set MCP_TRANSPORT=sse to use this mode (e.g. when running from PyCharm).
        print(f"Starting Lightup MCP server (SSE) on http://{host}:{port}/sse")
        mcp.run(transport="sse")
    else:
        # Default stdio mode: Claude Desktop spawns this process itself.
        mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
