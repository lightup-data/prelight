# Onboarding Guide

Welcome to Prelight! This guide takes you from a fresh clone to your first merged contribution. By the end you will have a working development environment, passing tests, and a mental map of the codebase.

> If you haven't yet, skim [ARCHITECTURE.md](ARCHITECTURE.md) first — it explains *what* Prelight is and *why* it's shaped this way. This document is about getting productive.

## 1. Prerequisites

| Tool | Version | Why |
|------|---------|-----|
| Python | 3.11+ | Required by `pyproject.toml` (`requires-python = ">=3.11"`) |
| git | any recent | Migration branches, commits, and PRs are core features |
| [uv](https://docs.astral.sh/uv/) (recommended) or pip | latest | Dependency management; `setup.sh` uses uv |
| `gh` CLI (optional) | latest | Used by `raise_pr` to open GitHub PRs; falls back to a compare URL without it |
| Docker (optional) | latest | Only needed for the isolated-session install mode |

## 2. Set up your development environment

```bash
git clone https://github.com/lightup-data/prelight.git
cd prelight

# With uv (recommended — same path the installer uses)
uv sync

# Or with plain pip
python -m venv .venv && source .venv/bin/activate
pip install -e ".[test]"
```

## 3. Run the tests

```bash
uv run pytest          # or just: pytest
```

All tests run against in-memory DuckDB and mocks — **no external services, credentials, or network access are needed**. They should pass out of the box. Test configuration lives in `pyproject.toml` (`testpaths = ["tests"]`, `pythonpath = ["."]`).

## 4. Run the server locally

Prelight reads its configuration from `config.yaml`, located either via the `PRELIGHT_CONFIG` environment variable or the current working directory. The repo ships a default DuckDB config (`config.yaml`), so this works immediately:

```bash
uv run prelight                 # starts the MCP server on stdio
MCP_TRANSPORT=sse uv run prelight   # or over SSE on 127.0.0.1:8000
```

To load demo data (an `analytics` schema with `customers` and `orders` tables):

```bash
uv run python setup/duckdb/init_local.py --path ~/.prelight/prelight.duckdb --schema analytics
# or, once connected through an AI client: "set up demo data"
```

To register the server with Claude Desktop / Claude Code so you can drive it conversationally:

```bash
uv run prelight install
```

This runs the interactive wizard in `prelight/cli/install.py`, writes `config.yaml`, and injects the MCP server entry into the Claude Desktop config (and `claude mcp add` for Claude Code).

### Trying the end-to-end flow

Once registered, open a new Claude session and type:

1. *"List my tables using prelight"*
2. *"Start a migration to add 10% markup to orders"*
3. *"Apply the transformation and run quality checks"*
4. *"Raise a PR"*

You'll watch the full 7-step flow (branch → sandbox → transform → checks → PR) described in [ARCHITECTURE.md](ARCHITECTURE.md#the-7-step-migration-flow).

## 5. Repo tour

```
prelight/
├── prelight/                  # The Python package
│   ├── server.py              # ★ MCP server: all 14 tools live here — start reading here
│   ├── config/
│   │   └── settings.py        # Pydantic config models, config.yaml loading/validation
│   ├── core/
│   │   ├── production_guard.py    # ★ SQL AST inspection that blocks production writes
│   │   ├── sandbox_manager.py     # In-memory registry of sandbox tables and their state
│   │   ├── quality_checks.py      # Executes checks, PASS/FAIL evaluation
│   │   ├── context_generator.py   # Generates context/{table}.md documentation
│   │   ├── sql_utils.py           # Identifier validation, slugs, sandbox→prod SQL rewrite
│   │   └── clients/
│   │       ├── __init__.py        # get_client() backend factory
│   │       ├── duckdb_client.py   # DuckDB backend (local dev default)
│   │       └── databricks_client.py  # Databricks backend (dual-token capable)
│   └── cli/
│       ├── install.py         # Interactive install wizard + MCP registration
│       ├── configure.py       # config.yaml writers (apply_duckdb / apply_databricks)
│       └── setup_demo.py      # Demo data loader for both backends
├── quality_checks/            # SQL check template library (categorical/numeric/integrity/volume)
├── setup/                     # Demo data SQL + DuckDB init script
├── tests/                     # pytest suite (see below)
├── config.yaml                # Default DuckDB config
├── setup.sh / setup-docker.sh # End-user installers
├── Dockerfile / entrypoint.sh # Isolated Docker session mode
└── pyproject.toml             # Package metadata, deps, entry point (prelight = prelight.server:main)
```

★ = the two files that matter most. `server.py` is the orchestration layer; `production_guard.py` is the safety core.

### Key dependencies

- **`mcp[cli]`** — FastMCP framework; tools are plain functions decorated with `@mcp.tool()`.
- **`sqlglot`** — SQL parsing. The production guard walks the parsed AST to find write targets.
- **`pydantic` + `pyyaml`** — config models and loading.
- **`duckdb`** / **`databricks-sql-connector`** — the two database backends.

## 6. Test suite map

| File | Covers |
|------|--------|
| `tests/conftest.py` | Fixtures: settings isolation (autouse), temp `config.yaml` writers |
| `tests/test_production_guard.py` | The safety core: every blocked statement type, sandbox/audit allowances, fail-closed parse errors |
| `tests/test_settings.py` | Config models, exactly-one-backend rule, token modes, caching |
| `tests/test_sql_utils.py` | Identifier validation (SQL injection rejection), slug, rewrite_to_production |
| `tests/test_duckdb_client.py` | Client operations against real in-memory DuckDB |
| `tests/test_quality_checks.py` | PASS/FAIL evaluation semantics |
| `tests/test_sandbox_manager.py` | Registry lifecycle (with mocked client) |
| `tests/test_context_generator.py` | Smart column descriptions, markdown generation/updating |

## 7. Making your first contribution

### Workflow

1. Create a branch from `master` (the default branch): `git checkout -b feat/my-change`
2. Make your change, **with tests**. Look at existing tests for the module you're touching and mirror their style.
3. Run `uv run pytest` — everything must pass.
4. Push and open a PR against `master`.

### Commit message convention

The repo uses conventional-commit-style prefixes scoped to the area touched. Recent examples from history:

```
feat(docker): add isolated Docker session mode for credential sandboxing
feat(list_tables): return full metadata in a single call
fix(visibility): flatten tool responses to single-line strings
```

### Conventions to follow

- **Tool responses are single-line strings** with emoji status prefixes: `✅` success, `❌` error, `🔒` guard message, `⚠️` warning. Fields are separated by ` | `. Keep new tool output consistent with this.
- **Every SQL execution path must pass through a guard.** If you add a tool that executes SQL, call `production_guard.check_sql()` (writes), `check_select_only()` (reads), or `check_not_ddl()` + `check_sql()` (previews) *before* touching the client. Never bypass it.
- **Validate all identifiers** that get interpolated into SQL with `sql_utils.validate_identifier()` — this is the injection defense.
- **Both clients must stay interface-compatible.** If you add a function to `duckdb_client.py`, add the same signature to `databricks_client.py` (and vice versa). `clients/__init__.py:get_client()` returns either module interchangeably.
- **Errors:** raise `ValueError` for validation problems, `RuntimeError` for client/config failures, `ProductionWriteBlockedError` for guard violations. Tools catch these and format user-facing `❌` messages.
- Type hints throughout; the codebase uses modern `str | None` union syntax (3.11+).

### Common contribution areas

| Want to... | Touch... |
|------------|----------|
| Add a new MCP tool | `prelight/server.py` (decorate with `@mcp.tool()`) |
| Add a quality check template | `quality_checks/{category}/your_check.sql` (use `{placeholder}` params; see [LOW_LEVEL_DESIGN.md](LOW_LEVEL_DESIGN.md#9-quality-check-template-library)) |
| Support a new database backend | New module in `prelight/core/clients/` implementing the common interface, plus config model in `settings.py` and a branch in `get_client()` |
| Improve column auto-descriptions | `context_generator.py:_smart_column_desc()` |
| Change guard behavior | `production_guard.py` — **add tests first**; this is the safety-critical core |

## 8. Things that surprise new developers

- **Session state is in-memory and per-process.** `_session_branch`/`_repo_root` in `server.py` and the sandbox `_registry` in `sandbox_manager.py` are module-level globals. Restarting the MCP server loses the session (sandboxes still exist in the database, but the server forgets them).
- **The guard fails closed.** If sqlglot can't parse a statement in `check_sql()`, the SQL is *blocked*, not allowed. This is intentional.
- **Migration SQL is stored rewritten to production names.** `apply_transformation` runs SQL against the sandbox but commits it to `migrations/*.sql` with the sandbox name rewritten back to the production table name — so the PR shows what would run against production.
- **Sandboxes are never auto-dropped.** There is no cleanup tool yet; `sbx_*` tables accumulate until manually dropped.
- **`list_tables` hides sandboxes.** Tables prefixed `sbx_` and the audit table are filtered out so the AI client only sees production tables.
- **Two protection layers on Databricks.** With dual-token config, production reads use a read-only credential and sandbox writes use a separate write credential — the SQL guard then becomes defense-in-depth rather than the only line of defense. On DuckDB, the SQL guard is the only line.

## 9. Getting help

- Read the [Low-Level Design](LOW_LEVEL_DESIGN.md) for exact function-by-function behavior.
- Check the [README troubleshooting section](../README.md) for runtime/setup issues.
- Open a GitHub issue for bugs or design questions.
