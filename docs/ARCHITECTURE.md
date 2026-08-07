# Architecture (High-Level Design)

This document explains what Prelight is, the system's shape, the core safety model, and the design decisions behind it. For function-level detail, see [LOW_LEVEL_DESIGN.md](LOW_LEVEL_DESIGN.md).

## 1. What Prelight is

Prelight is an **MCP (Model Context Protocol) server** that sits between an AI client (Claude Desktop / Claude Code) and a production database (DuckDB or Databricks). Its job is to make AI-driven data transformations safe:

- The AI **never writes to production**. Every transformation runs against a sandbox copy of the table.
- Every change is **quality-checked** (sandbox vs. production comparisons).
- Every change ships as a **git branch + GitHub PR** that a human reviews and approves.

The human's role is reduced to two actions: describe the change in natural language, and approve the PR at the end.

## 2. System context

```
┌──────────────────┐   MCP (stdio/SSE)   ┌──────────────────────┐
│   AI Client      │◄───────────────────►│   Prelight Server     │
│ (Claude Desktop/ │   14 tools,         │   (FastMCP, Python)   │
│  Claude Code)    │   2 prompts         │                       │
└──────────────────┘                     └──────┬───────┬────────┘
                                                │       │
                                   SQL (guarded)│       │ subprocess
                                                ▼       ▼
                                  ┌──────────────┐   ┌──────────────────┐
                                  │   Database   │   │  git / gh CLI    │
                                  │ DuckDB or    │   │  branches,       │
                                  │ Databricks   │   │  commits, PRs    │
                                  └──────────────┘   └──────────────────┘
```

The AI client connects over stdio (default, used by Claude Desktop) or SSE (`MCP_TRANSPORT=sse`, host/port via `MCP_HOST`/`MCP_PORT`). The server talks to exactly one database backend at a time (chosen in `config.yaml`) and shells out to `git`/`gh` for version-control operations in the *user's* project repository.

## 3. Layered architecture

```
┌────────────────────────────────────────────────────────────┐
│  Tool layer — prelight/server.py                           │
│  14 MCP tools + 2 prompts. Orchestration, session state,   │
│  git operations, user-facing message formatting.           │
├────────────────────────────────────────────────────────────┤
│  Core layer — prelight/core/                               │
│  production_guard   SQL AST inspection (the safety core)   │
│  sandbox_manager    in-memory sandbox registry             │
│  quality_checks     check execution + PASS/FAIL semantics  │
│  context_generator  table documentation generation         │
│  sql_utils          identifier validation, SQL rewriting   │
├────────────────────────────────────────────────────────────┤
│  Client layer — prelight/core/clients/                     │
│  Common 9-function interface; get_client() returns the     │
│  duckdb_client or databricks_client module per config.     │
├────────────────────────────────────────────────────────────┤
│  Config layer — prelight/config/settings.py                │
│  Pydantic models, config.yaml discovery, cached singleton. │
└────────────────────────────────────────────────────────────┘
```

Dependencies point strictly downward: tools call core, core calls clients, everyone reads config. Clients never call upward.

## 4. The 7-step migration flow

This is the product's central workflow. Each step maps to one or more MCP tools in `server.py`:

```
 1. start_migration        Cut git branch  migration/{slug}-{YYYYMMDD-HHMM}
         │                 (sets session state: _session_branch, _repo_root)
 2. list_tables /          Explore production — read-only;
    describe_table /       query_table is hard-limited to SELECT
    query_table
         │
 3. create_sandbox         CREATE TABLE sbx_{table}_{YYYYMMDD_HHMM}
         │                 AS SELECT * FROM {table}
 4. apply_transformation   Run SQL on the sandbox (guard-checked),
         │                 commit migrations/{ts}-{table}.sql to the branch
 5. save_quality_checks    Persist check SQL to quality_checks/runs/{sandbox}/,
         │                 commit
 6. run_quality_checks     Execute checks (sandbox vs production),
         │                 write context/{table}.md + MIGRATION_NOTES.md, commit
 7. raise_pr               git push + `gh pr create` (PR body = MIGRATION_NOTES.md)
```

**Artifacts on the migration branch** (all in the user's repo, all reviewable in the PR):

| Path | Written by | Contents |
|------|------------|----------|
| `migrations/{ts}-{table}.sql` | `apply_transformation` | The transformation SQL, rewritten to production table names |
| `quality_checks/runs/{sandbox}/*.sql` | `save_quality_checks` | One file per quality check |
| `context/{table}.md` | `run_quality_checks` | Auto-generated table documentation (human edits preserved on update) |
| `MIGRATION_NOTES.md` | `run_quality_checks` | Accumulated report: SQL + check results per table; becomes the PR body |

**Ordering enforcement** is deliberately light: exploration tools work any time, but `create_sandbox` and `raise_pr` require `start_migration` to have run (session state check), and `apply_transformation`/`run_quality_checks` require the sandbox to exist in the registry.

## 5. The safety model

Production protection is the reason Prelight exists. There are two independent mechanisms:

### Layer 1 — SQL inspection (always active)

`prelight/core/production_guard.py` parses every statement with **sqlglot** and walks the AST:

- `check_sql()` — used by `apply_transformation`. Finds the *write target* of INSERT / UPDATE / DELETE / MERGE / DROP / ALTER / TRUNCATE / CREATE-OR-REPLACE statements. If the target table does not start with the sandbox prefix (`sbx_` by default) and is not the audit table, it raises `ProductionWriteBlockedError`.
- `check_select_only()` — used by `query_table`. Allows only SELECT / WITH-SELECT.
- `check_not_ddl()` — used by `preview_transformation`. Additionally blocks DDL so previews can never mutate schema.

**Fail-closed:** if sqlglot cannot parse the SQL in `check_sql()`/`check_select_only()`, the statement is *blocked*. Unparseable SQL never reaches the database through a write path.

### Layer 2 — Credential separation (Databricks dual-token mode)

When configured with two tokens, the Databricks client maintains two connections:

- **prod connection** (`prod_token`, read-only credential) — used by `execute_query`, i.e. all reads.
- **sandbox connection** (`sandbox_token`, write access scoped to `sbx_*` tables) — used by `execute_statement`, i.e. all writes.

In this mode the *database itself* refuses production writes regardless of what SQL reaches it; the AST guard becomes defense-in-depth. In single-token mode (and always on DuckDB, which has one connection), Layer 1 is the sole protection.

### Supporting defenses

- `sql_utils.validate_identifier()` rejects any table/schema name containing characters outside `[A-Za-z0-9_.]` before it is interpolated into SQL — the injection defense for identifiers.
- `list_tables` filters out `sbx_*` and the audit table, so the AI's view of "the tables" is production-only.
- Guard status messages (prefixed `🔒`) are emitted to the user on every sandbox creation and transformation, making the protection mechanism visible and auditable in the conversation.

## 6. State model

Prelight is intentionally light on persistence:

| State | Where | Lifetime |
|-------|-------|----------|
| Migration session (`_session_branch`, `_repo_root`) | Module globals in `server.py` | MCP server process |
| Sandbox registry (`_registry: dict[str, SandboxRecord]`) | Module global in `sandbox_manager.py` | MCP server process |
| Settings cache (`_settings`) | Module global in `settings.py` | Process, reset on backend switch |
| DB connections | Module globals in each client | Process, reset on backend switch |
| Sandbox tables (`sbx_*`) | The database | Until manually dropped (no auto-cleanup) |
| Migration artifacts | Git branch in the user's repo | Permanent (reviewable history) |

Consequence: restarting the server forgets in-flight sessions and sandboxes (the tables survive in the database, the registry doesn't). Durable state lives in **git** — that is a deliberate choice: the branch *is* the audit trail.

## 7. Backend abstraction

`prelight/core/clients/__init__.py:get_client()` returns one of two modules implementing the same 9-function interface:

```
execute_query(sql) -> list[dict]          # reads
execute_statement(sql) -> None            # writes/DDL
get_table_schema(table) -> list[dict]
get_row_count(table) -> int
list_table_names(schema) -> list[str]
list_tables_with_metadata(schema, table_names) -> list[dict]
create_sandbox_table(src, dst) -> None
table_exists(table) -> bool
reset_connection() -> None
```

| | DuckDB | Databricks |
|---|--------|-----------|
| Use case | Local dev, demo, zero-setup | Production data platform |
| Connections | One persistent connection | Two (prod read / sandbox write) in dual-token mode |
| Safety | SQL guard only | SQL guard + credential separation |
| Config | `path` (file or in-memory), `schema` | `host`, `http_path`, `catalog`, `schema`, tokens |

Exactly one backend is active at a time — `settings.py` validates "exactly one of `duckdb`/`databricks`" at load. The `configure_duckdb` / `configure_databricks` tools rewrite `config.yaml` and reset all caches/connections to switch live.

## 8. Quality check system

Checks are plain SQL chosen/parameterized by the AI from the template library in `quality_checks/` (4 categories: volume, numeric, categorical, integrity — see the [full catalog](LOW_LEVEL_DESIGN.md#9-quality-check-template-library)). Two result conventions are supported:

1. **Status pattern** (preferred): the query returns row(s) with a `status` column valued `'PASS'`/`'FAIL'`.
2. **Violation pattern**: the query returns *violating rows* — zero rows means PASS, any rows means FAIL.

`quality_checks._evaluate_result()` implements exactly these semantics. Checks generally compare the sandbox against the production original (drift, row counts, referential integrity), which is possible because both live in the same schema.

## 9. Configuration

`config.yaml` is found via the `PRELIGHT_CONFIG` env var (how Claude Desktop/Code point at it) or the current working directory. Pydantic models validate it at server startup — the server exits early with a friendly message on bad config. Key options:

```yaml
duckdb:                      # OR databricks: (exactly one)
  path: "~/.prelight/prelight.duckdb"   # omit for in-memory
  schema: "analytics"
  sandbox_prefix: "sbx_"
  audit_table: "qg_quality_results"
quality:
  row_count_drift_pct: 5
```

Databricks adds `host`, `http_path`, `catalog`, and either `token` (legacy single-token) or `prod_token` + `sandbox_token` (recommended dual-token).

## 10. Deployment modes

1. **Standard install** (`setup.sh`): clones to `~/.prelight`, `uv sync`, writes a DuckDB config, loads demo data, registers the MCP server with Claude Desktop/Code. The server runs as a child process of the AI client over stdio.
2. **Docker isolated session** (`setup-docker.sh`): builds an image containing Python, Prelight, git, `gh`, and the Claude Code CLI. The `prelight-session` launcher runs Claude Code *inside* the container where Prelight is the **only** MCP server and the container only sees mounted volumes (`~/.prelight`, project dir, git/gh credentials). This is credential sandboxing: the AI session cannot reach other MCP servers or the broader filesystem.
3. **SSE server**: `MCP_TRANSPORT=sse` runs a standalone HTTP/SSE server for clients that connect over the network.

## 11. Key design decisions

| Decision | Rationale |
|----------|-----------|
| **Sandbox-first, never write production** | The core product invariant. Cheap on modern engines (CTAS copy), eliminates the worst failure mode entirely. |
| **AST-based guard, not regex/keyword matching** | sqlglot parsing finds the real write target, handling CTEs, schema-qualified and quoted names; keyword matching is trivially bypassable. |
| **Fail closed on parse errors** | Unparseable SQL on a write path is blocked. Safety beats convenience. |
| **Git as the durable store** | Branch + commits + PR give review, audit history, and rollback for free; no extra database tables or services needed. |
| **Module-level clients with a common interface** | Adding a backend means adding one module + one config model; the tool layer is backend-agnostic. |
| **In-memory session state** | Sessions are short-lived and conversational; the valuable artifacts are committed to git anyway. |
| **Single-line tool responses with emoji prefixes** | MCP tool output is rendered in chat; one-line `✅ ... | field | field` strings stay readable and machine-scannable. |
| **Stored migration SQL is rewritten to production names** | The PR should show reviewers what will run against *production*, not sandbox names with timestamps. |

## 12. Known limitations / future work

- No sandbox cleanup tool — `sbx_*` tables accumulate.
- Sandbox registry is not persisted; a server restart orphans in-flight sandboxes.
- `raise_pr` opens the PR but nothing automates *applying* the migration to production after approval — that step is currently up to the team's own deployment process.
- Gemini CLI and Codex CLI client support is on the roadmap (per README).
