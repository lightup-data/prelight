# Low-Level Design

Module-by-module reference for contributors. Line numbers are approximate and may drift as the code evolves; names are the stable anchors. For the system-level picture, read [ARCHITECTURE.md](ARCHITECTURE.md) first.

## Contents

1. [`prelight/server.py` — MCP tool layer](#1-prelightserverpy--mcp-tool-layer)
2. [`prelight/core/production_guard.py` — safety core](#2-prelightcoreproduction_guardpy--safety-core)
3. [`prelight/core/sandbox_manager.py` — sandbox registry](#3-prelightcoresandbox_managerpy--sandbox-registry)
4. [`prelight/core/quality_checks.py` — check execution](#4-prelightcorequality_checkspy--check-execution)
5. [`prelight/core/context_generator.py` — table docs](#5-prelightcorecontext_generatorpy--table-docs)
6. [`prelight/core/sql_utils.py` — SQL utilities](#6-prelightcoresql_utilspy--sql-utilities)
7. [`prelight/core/clients/` — database backends](#7-prelightcoreclients--database-backends)
8. [`prelight/config/settings.py` — configuration](#8-prelightconfigsettingspy--configuration)
9. [Quality check template library](#9-quality-check-template-library)
10. [`prelight/cli/` — install, configure, demo](#10-prelightcli--install-configure-demo)
11. [Packaging, installers, Docker](#11-packaging-installers-docker)
12. [Error-handling conventions](#12-error-handling-conventions)

---

## 1. `prelight/server.py` — MCP tool layer

### Server setup

```python
mcp = FastMCP("prelight",
              host=os.environ.get("MCP_HOST", "127.0.0.1"),
              port=int(os.environ.get("MCP_PORT", "8000")))
```

`main()` (the `prelight` console-script entry point) dispatches on `sys.argv[1]`: `install` → install wizard, `setup-demo` → demo loader, otherwise it validates settings via `get_settings()` (exiting on bad config) and runs the MCP server — `transport="sse"` if `MCP_TRANSPORT=sse`, else stdio.

### Session state (module globals)

```python
_session_branch: str | None = None   # set by start_migration
_repo_root: str | None = None        # set by start_migration
```

Set by `start_migration`; required by `create_sandbox` and `raise_pr`; checked by `apply_transformation` / `save_quality_checks` / `run_quality_checks` to decide whether to write files and commit (the DB work still happens without it).

### Helpers

| Helper | Behavior |
|--------|----------|
| `_run_git(*args, cwd)` | `subprocess.run(["git", "-C", cwd, ...])` → `(success: bool, stdout: str)` |
| `_detect_base_branch(repo_root)` | Tries `origin/HEAD` symbolic ref → local `main` → local `master` → `None` (fresh repo) |
| `_fmt_rows(rows, limit=50)` | Renders `list[dict]` as ASCII table, dynamic column widths, 50-row cap |
| `_commit_and_get_hash(msg, cwd)` | `git commit -m msg` → `(success, short_hash)` |

### Tools (14)

All tools return a single-line `str`. Errors are returned as `❌ ...` strings, not raised across the MCP boundary.

#### `start_migration(description, working_directory, init_git=False)`
Validates git is installed; optionally `git init`s (with an empty initial commit); resolves the repo root via `git rev-parse --show-toplevel`; stashes tracked changes; checks out the detected base branch; creates and checks out `migration/{slug(description)}-{YYYYMMDD-HHMM}`; sets `_session_branch` and `_repo_root`. If the directory isn't a git repo and `init_git` is false, returns a prompt asking the user whether to initialize one.

#### `list_tables()`
`client.list_table_names(schema)` filtered to exclude names starting with the sandbox prefix and the audit table, then `client.list_tables_with_metadata(...)` for row counts + columns. One tool call returns everything (schema, counts, columns) — by design, to avoid N round trips.

#### `describe_table(table)`
`client.get_table_schema(table)` + `client.get_row_count(table)` for one table.

#### `query_table(sql)`
Guard: `production_guard.check_select_only(sql)` — SELECT/WITH-SELECT only. Executes via `client.execute_query()`, formats with `_fmt_rows()` (50-row cap).

#### `create_sandbox(table)`
Requires `_session_branch`. Delegates to `sandbox_manager.create_sandbox(table)` (which calls `client.create_sandbox_table`). Returns sandbox name, row count, columns, and a `🔒` identity-guard message: credential-based wording when Databricks dual-token mode is on, SQL-inspection wording otherwise.

#### `apply_transformation(sandbox_name, sql)`
The write path. Sequence:
1. `sandbox_manager.get_sandbox(sandbox_name)` — ValueError if unknown.
2. **`production_guard.check_sql(sql, sandbox_prefix, audit_table)`** — raises `ProductionWriteBlockedError` if any statement writes a production table.
3. `client.execute_statement(sql)`.
4. `sandbox_manager.log_transformation(...)` appends to the record's `applied_sqls`.
5. If a session is active: `sql_utils.rewrite_to_production()` rewrites sandbox names → production names, writes/appends `migrations/{YYYYMMDD-HHMMSS}-{table}.sql`, and commits (`migration({table}): add transformation`).
6. Returns result + `🔒` guard message.

#### `preview_transformation(sql)`
Non-persistent SELECT against the sandbox. Guards: `check_not_ddl(sql)` **and** `check_sql(sql, ...)` — a preview can never run DDL or write production.

#### `save_quality_checks(sandbox_name, checks)`
`checks` is `list[dict]`, each `{name, sql, description}` (validated). Stored on the record as `custom_quality_checks`; if a session is active, each check is written to `quality_checks/runs/{sandbox_name}/{name}.sql` with a header comment and committed (`quality_checks({table}): save N check definitions`). The docstring instructs the AI on the two result patterns (status column / violation rows) and explicitly forbids bare `COUNT(*)` checks without a status column.

#### `run_quality_checks(sandbox_name)`
Requires saved checks (ValueError otherwise). Calls `quality_checks.run_custom_checks(...)`; stores results via `sandbox_manager.store_quality_results()`; marks `quality_passed` if all passed. If a session is active, writes and commits three artifacts:
1. `context/{table}.md` — via `context_generator.build_context_md()` (preserves human edits on re-run). Commit: `context({table}): update table context`.
2. `MIGRATION_NOTES.md` — appends a section per table: transformation SQL block + per-check ✅/❌ results. Commit: `notes({table}): add quality check results`.

#### `raise_pr(description)`
Requires session state. Checks `git remote get-url origin` (returns manual push instructions if absent); reads `MIGRATION_NOTES.md` as the PR body; `git push -u origin {branch}`; then `gh pr create --title "[Migration] {description}" --body ... --base {detected} --head {branch}`. If `gh` is unavailable/fails, returns a GitHub compare URL (`.../compare/{branch}?expand=1&title=...`) as fallback.

#### `configure_duckdb(path="", schema="analytics")` / `configure_databricks(host, http_path, token="", prod_token="", sandbox_token="", schema="analytics", catalog="hive_metastore")`
Rewrite `config.yaml` via `cli/configure.py` (`apply_duckdb` / `apply_databricks`), then `clients.reset_all()` to drop cached settings and connections. `configure_duckdb` auto-loads demo data into a new/empty database file. `configure_databricks` validates dual-token XOR single-token and normalizes the host to `https://`.

#### `ingest_csv(table_name, url)`
DuckDB only. Validates `table_name` with `validate_identifier()`, refuses to overwrite an existing table, regex-validates the URL (no shell metacharacters), installs the `httpfs` extension for remote URLs, then `CREATE TABLE ... AS SELECT * FROM read_csv_auto(...)`.

#### `setup_demo()`
Delegates to `cli/setup_demo.run_setup_demo_core()`. Idempotent — drops and recreates the demo tables.

### Prompts (2)

- `set_up_demo_data_assets` — nudges the model to call `setup_demo()`.
- `walk_me_through_an_end_to_end_transformation` — scripted walkthrough of the full 7-step flow.

---

## 2. `prelight/core/production_guard.py` — safety core

```python
class ProductionWriteBlockedError(Exception): ...
```

| Function | Used by | Behavior |
|----------|---------|----------|
| `check_sql(sql, sandbox_prefix, audit_table)` | `apply_transformation`, `preview_transformation` | Parses with sqlglot (`error_level=WARN`); for each statement, extracts the write target of INSERT/UPDATE/DELETE/MERGE/DROP/ALTER/CREATE-OR-REPLACE (TRUNCATE checks **all** `exp.Table` nodes); raises if the target is production. SELECT/WITH pass silently. **Parse failure ⇒ raise (fail closed).** |
| `check_not_ddl(sql)` | `preview_transformation` | Raises on ALTER/DROP/CREATE/TRUNCATE. Parse failure ⇒ pass silently (execute_query will surface the real error; previews are read-only anyway given `check_sql` also runs). |
| `check_select_only(sql)` | `query_table` | Only SELECT or WITH-SELECT allowed; everything else (and parse failures) raises. |

Internals:
- `_is_production(table_name, sandbox_prefix, audit_table)` — strips quotes/backticks, lowercases; production ⇔ not `sbx_`-prefixed and not the audit table. Comparison is case-insensitive.
- `_extract_target_table(statement)` — pulls the target `exp.Table` out of write-statement nodes (handles `exp.Schema` wrappers, schema-qualified and quoted names).

Error message format (returned verbatim to the user):
```
🚫 Production write blocked: [INSERT] on [orders] is not allowed. All changes must go
through a sandbox table (prefix: sbx_). Use create_sandbox first.
```

**Contributor rule:** any change here needs tests in `tests/test_production_guard.py` *first*. This module is the product's safety invariant.

---

## 3. `prelight/core/sandbox_manager.py` — sandbox registry

```python
@dataclass
class SandboxRecord:
    sandbox_name: str
    source_table: str
    created_at: datetime
    applied_sqls: list[str]
    quality_passed: bool
    quality_run_id: str | None
    quality_check_results: list[dict]
    custom_quality_checks: list[dict]
    schema_columns: list[dict]
    migration_file_path: str | None

_registry: dict[str, SandboxRecord] = {}   # in-memory, per-process
```

| Function | Behavior |
|----------|----------|
| `create_sandbox(source_table)` | Builds `{prefix}{source_table}_{YYYYMMDD_HHMM}`; calls `client.create_sandbox_table(f"{schema}.{source_table}", f"{schema}.{sandbox_name}")`; captures the schema; registers and returns the record |
| `get_sandbox(sandbox_name)` | Registry lookup; `ValueError` if missing (this is the "sandbox must exist" enforcement used by tools) |
| `get_sandbox_for_table(source_table)` | Most recent sandbox for a production table, or `None` |
| `log_transformation(sandbox_name, sql)` | Appends to `applied_sqls` |
| `store_quality_results(sandbox_name, run_id, checks)` | Saves run id + per-check results |
| `mark_quality_passed(sandbox_name)` | Sets `quality_passed = True` |
| `list_sandboxes()` | All records |

No persistence and no cleanup: a server restart empties the registry (tables survive in the DB); `sbx_*` tables are never dropped automatically.

---

## 4. `prelight/core/quality_checks.py` — check execution

`run_custom_checks(sandbox_name, source_table, custom_checks) -> dict`
Executes each check's SQL via `client.execute_query()`, evaluates, and returns:

```python
{"run_id": <uuid>, "sandbox_name": ..., "source_table": ...,
 "checks": [{"check", "status", "result", "expected", "detail", "sql"}, ...],
 "all_passed": bool}
```

`_evaluate_result(rows) -> (status, result_str)` — the PASS/FAIL semantics:

| Result shape | Verdict |
|--------------|---------|
| 0 rows | **PASS** (violation pattern: nothing violated) |
| Rows with a `status` column | **FAIL** if any row's status is `FAIL` (case-insensitive), else PASS |
| Rows without a `status` column | **FAIL** (the rows *are* the violations) |

`_extract_failure_detail(rows)` formats the first 3 failing rows; `_fmt_result_rows(rows, limit=10)` renders `col=val` pairs with a `(... N more rows)` suffix.

---

## 5. `prelight/core/context_generator.py` — table docs

Entry point: `build_context_md(source_table, schema, schema_columns, description, iso_ts, context_notes=None, existing_content=None) -> str`

- `existing_content is None` → `_build_fresh()`: full template with YAML frontmatter (`table`, `schema`, `last_updated`), a Purpose section (from `context_notes` or a fill-me-in prompt), a Columns markdown table with **inferred descriptions**, and an empty Metrics section.
- `existing_content` provided → `_update_existing()`: updates only the `last_updated` timestamp and appends rows for *new* columns — **human edits to descriptions/purpose/metrics are preserved**.

`_smart_column_desc(column_name, data_type)` infers descriptions by name pattern: `id` / `*_id` (identifiers/FKs), `*_status`, money words (`amount`, `price`, `cost`, `revenue`, `total`), timestamps (`created_at`, `updated_at`, `deleted_at`, generic `*_at`/`*_date`/`*_time`), `type`/`kind`/`category`, boolean flags (`is_*`, `has_*`, `*flag`), ratios (`pct`, `percent`, `rate`, `ratio`), counts, name/label fields, contact fields, geography fields. Unknown columns get an italicized prompt asking the human to describe them.

Section surgery helpers: `_get_section_body`, `_replace_section_body`, `_documented_column_names` (parses the existing markdown table), `_append_new_columns`.

---

## 6. `prelight/core/sql_utils.py` — SQL utilities

| Function | Behavior |
|----------|----------|
| `validate_identifier(name, label="identifier")` | Regex allowlist (alphanumeric, underscore, dots for qualification). Raises `ValueError` on anything else — semicolons, quotes, spaces, backticks, parens, leading digits, control characters. **Call this on every identifier interpolated into SQL.** |
| `slug(text, max_len=40)` | Lowercase kebab-case for branch names (`"Apply 10% markup"` → `apply-10-markup`) |
| `rewrite_to_production(sql, schema, sandbox_name, source_table)` | Case-insensitive, word-boundary regex replace of `{schema}.{sandbox_name}` → `{schema}.{source_table}`, then bare `{sandbox_name}` → `{source_table}`. Used so committed migration SQL reads in production terms. |

---

## 7. `prelight/core/clients/` — database backends

### `__init__.py` — factory

- `get_client()` → returns the `duckdb_client` or `databricks_client` **module** based on `settings.backend`.
- `reset_all()` → `reset_settings()` + both clients' `reset_connection()`. Called when switching backends.

### Common interface (both modules implement exactly this)

```python
execute_query(sql) -> list[dict]
execute_statement(sql) -> None
get_table_schema(table) -> list[dict]        # [{column_name, data_type, nullable}]
get_row_count(table) -> int
list_table_names(schema) -> list[str]
list_tables_with_metadata(schema, table_names) -> list[dict]
create_sandbox_table(source_table, sandbox_table) -> None
table_exists(table) -> bool
reset_connection() -> None
```

### `duckdb_client.py`

- One module-level `_connection`, lazily created from `settings.duckdb.path` (default in-memory).
- `create_sandbox_table` runs `CREATE SCHEMA IF NOT EXISTS` then `CREATE TABLE {dst} AS SELECT * FROM {src}`.
- `list_tables_with_metadata` is deliberately **2 queries total**: one `information_schema.columns` fetch for all tables, one `UNION ALL` of `COUNT(*)`s — not N+1.
- `list_table_names` filters `information_schema.tables` to `BASE TABLE`.

### `databricks_client.py`

- Two module-level connections: `_prod_connection` / `_sandbox_connection`, built by `_make_connection(token)` with `host`/`http_path`/`catalog` from settings.
- `get_prod_connection()` uses `settings.databricks.effective_prod_token`; `get_sandbox_connection()` uses `effective_sandbox_token`. In single-token mode both properties return the same token, so the two connections are credential-identical.
- **Read/write split:** `execute_query`, `get_row_count` → prod connection; `execute_statement`, `create_sandbox_table` → sandbox connection.
- `get_table_schema` parses `DESCRIBE TABLE`, skipping `#`-prefixed metadata rows. `list_table_names` uses `SHOW TABLES IN {schema}`; `table_exists` uses `SHOW TABLES ... LIKE`.

---

## 8. `prelight/config/settings.py` — configuration

Pydantic v2 models:

| Model | Fields (defaults) |
|-------|-------------------|
| `DuckDBConfig` | `path: str\|None` (None = in-memory), `schema="analytics"`, `sandbox_prefix="sbx_"`, `audit_table="qg_quality_results"` |
| `DatabricksConfig` | `host` (must be `https://`), `http_path`, `schema`, `catalog="hive_metastore"`, `sandbox_prefix`, `audit_table`, plus tokens (below) |
| `QualityConfig` | `row_count_drift_pct=5` |
| `Settings` | `databricks: DatabricksConfig\|None`, `duckdb: DuckDBConfig\|None`, `quality: QualityConfig` |

**Token validation** (`DatabricksConfig.check_tokens`): exactly one of — dual-token pair (`prod_token` + `sandbox_token`) **or** legacy single `token`. Properties `effective_prod_token` / `effective_sandbox_token` fall back to `token`; `dual_token_mode` is the boolean the server uses to choose the guard message wording.

**Backend validation** (`Settings.check_exactly_one_backend`): exactly one of `duckdb`/`databricks`. Convenience properties on `Settings`: `backend`, `db_schema`, `sandbox_prefix`, `audit_table` — these delegate to the active backend so callers never branch.

**Loading:** `_find_config_path()` checks `PRELIGHT_CONFIG` env var, then `./config.yaml`. `get_settings()` loads, validates, caches in module-level `_settings`, and raises `RuntimeError` with actionable messages (file missing / empty / bad YAML / validation failure). `reset_settings()` clears the cache — used by the `configure_*` tools and the test suite's autouse fixture.

---

## 9. Quality check template library

Location: `quality_checks/{category}/{check}.sql`. Templates use `{placeholder}` parameters that the AI substitutes before calling `save_quality_checks`. Two result conventions (see [§4](#4-prelightcorequality_checkspy--check-execution)).

| Category | Template | Parameters | Pass condition | Pattern |
|----------|----------|------------|----------------|---------|
| volume | `row_count_comparison.sql` | schema, source_table, sandbox_name | Row-count drift ≤ 10% | status |
| volume | `new_rows_inspection.sql` | schema, sandbox_name, source_table, primary_key_column | Informational (always PASS; shows up to 50 new rows) | status |
| numeric | `mean_drift.sql` | + numeric_column | AVG relative drift ≤ 10% | status |
| numeric | `sum_comparison.sql` | + numeric_column | SUM drift ≤ 10% | status |
| numeric | `min_max_bounds.sql` | + numeric_column | sbx min ≥ prod min and sbx max ≤ prod max × 1.1 | status |
| categorical | `distribution_shift.sql` | + category_column | Per-category share shift ≤ 5 pp | status |
| categorical | `value_set_drift.sql` | + category_column | No new category values in sandbox | violation rows |
| integrity | `uniqueness_on_keys.sql` | schema, sandbox_name, key_column | No duplicate key values | violation rows |
| integrity | `foreign_key_integrity.sql` | schema, sandbox_name, referenced_table, fk_column, referenced_pk_column | No orphaned FKs | violation rows |
| integrity | `referential_completeness.sql` | schema, source_table, sandbox_name, key_column | No production rows missing from sandbox (LIMIT 50 shown) | violation rows |

**Adding a template:** create `quality_checks/{category}/{name}.sql`, use `{placeholder}` params, and follow one of the two result patterns. Prefer the status pattern for aggregate comparisons and the violation pattern for row-level integrity checks.

Executed check runs are saved separately under `quality_checks/runs/{sandbox_name}/` in the *user's* repo — don't confuse the library (this repo) with run artifacts (user repos).

---

## 10. `prelight/cli/` — install, configure, demo

### `install.py` — `prelight install`

Two paths:
- **Fast path:** `config.yaml` already exists → skip prompts, just (re-)register the MCP server.
- **Full wizard:** choose backend (Databricks / new DuckDB / existing DuckDB); `_collect_databricks_config()` (auto-detects `~/.databrickscfg`, offers dual-token setup, masks tokens with `_mask()`) or `_collect_duckdb_config()`; writes `config.yaml`; loads demo data for new DuckDB files.

Registration: locates the Claude Desktop config per-OS (`_find_claude_desktop_config()` — macOS / Windows / Linux XDG paths) and injects:

```json
{"mcpServers": {"prelight": {"command": "<venv>/bin/prelight",
                             "env": {"PRELIGHT_CONFIG": "<path>/config.yaml"}}}}
```

For Claude Code it shells out to `claude mcp add`. Path discovery: `_get_project_root()` / `_get_command_path()` walk up from the venv's bin directory.

### `configure.py`

`apply_duckdb(...)` / `apply_databricks(...)` rewrite `config.yaml` (each removes the other backend's section — preserving the exactly-one-backend invariant). Called by the `configure_duckdb` / `configure_databricks` MCP tools and the wizard.

### `setup_demo.py`

`run_setup_demo_core()` detects the backend and executes the matching `setup/{backend}/demo_data.sql`, splitting on `;` (`_split_statements()` skips comment-only blocks) and substituting the schema name if non-default. Demo data: `analytics.customers` (10 rows) and `analytics.orders` (50 rows; statuses completed/pending/shipped/cancelled; no NULLs or duplicate keys — intentionally clean so demo quality checks pass).

---

## 11. Packaging, installers, Docker

### `pyproject.toml`

- Package `prelight` v0.1.0, Python ≥ 3.11.
- Deps: `mcp[cli]`, `databricks-sql-connector`, `duckdb`, `sqlglot`, `pyyaml`, `pydantic` (v2). Test extra: `pytest`.
- Console script: `prelight = "prelight.server:main"`.
- pytest config: `testpaths=["tests"]`, `pythonpath=["."]`.

### `setup.sh` (standard install)

check git → install uv if missing → clone/update repo to `~/.prelight` (override with `PRELIGHT_INSTALL_DIR`) → `uv sync` → write DuckDB `config.yaml` → load demo data (`setup/duckdb/init_local.py`) → `prelight install` to register with Claude clients. `setup-docker.sh` simply re-invokes `setup.sh --docker`.

### Docker mode (`Dockerfile` + `entrypoint.sh`)

Image: `python:3.12-slim` + git + Node.js + `gh` CLI + Claude Code CLI (`npm install -g @anthropic-ai/claude-code`) + Prelight via uv. `entrypoint.sh` modes:

| Arg | Behavior |
|-----|----------|
| `mcp-server` | Runs `uv run --directory /app prelight` (for Claude Desktop connecting to a containerized server) |
| `setup-demo` | One-shot demo data load during install |
| *(default)* | Interactive isolated session: builds `/root/.claude.json` from the host's auth (mounted as `.claude.json.host`), **strips all host MCP servers**, injects prelight as the only one, then launches `claude` |

The installer drops a `prelight-session` launcher into `~/.local/bin` that `docker run`s the image with mounts for `~/.prelight`, the current project directory, `~/.claude`, `~/.gitconfig`, `~/.ssh`, and `~/.config/gh` — enough to commit/push/PR from inside, nothing more.

### `setup/duckdb/init_local.py`

Standalone demo initializer: argparse `--path` / `--schema` (falls back to `config.yaml` / env), creates parent dirs, executes the split demo SQL, prints row counts.

---

## 12. Error-handling conventions

| Exception | Meaning | Raised by |
|-----------|---------|-----------|
| `ProductionWriteBlockedError` | Guard violation | `production_guard` |
| `ValueError` | Validation failure (unknown sandbox, bad identifier, malformed check dict) | core modules |
| `RuntimeError` | Config/client failure (bad config.yaml, connection errors, git failures) | settings, clients, server |

Tools catch specific exceptions first, generic `Exception` last, and convert everything to single-line strings:

- `✅` success (followed by ` | `-separated fields and next-step guidance)
- `❌` error
- `🚫` blocked by guard (the guard's message is preserved verbatim)
- `🔒` guard/identity status (always shown after sandbox creation and transformations)
- `⚠️` warning / user decision needed

Exceptions never propagate across the MCP boundary — a raised exception inside a tool would surface as an opaque protocol error in the client, so everything is stringified.
