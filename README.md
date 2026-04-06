# Prelight

Transforming production data is risky. One wrong query and it's gone. Prelight fixes that — every change your AI client makes goes into a sandbox first, gets quality-checked, and ships as a PR you approve. Production is never touched.

Just talk to your AI client. Prelight handles the rest.

Works with Claude Desktop and Claude Code today — Gemini CLI and Codex CLI coming soon.

---

## Get Started in 60 Seconds

Requires Python 3.11+ and Claude Desktop or Claude Code.

```bash
curl -sL https://raw.githubusercontent.com/lightup-data/prelight/master/setup.sh | bash
```

Claude will have live tables ready to work with the moment setup finishes.

**Claude Desktop:** fully quit and reopen, then start a new conversation.  
**Claude Code:** open a new session.

Type: **List my tables using prelight**

---

## How It Works

Every transformation follows the same 7-step flow. Claude runs all of it — you just describe what you want and approve the PR at the end.

```
 1. start_migration       Create a git branch for this change
        │
 2. list_tables           See what production tables exist
    describe_table        Inspect schema and row count
    query_table           Read production data safely (SELECT only)
        │
 3. create_sandbox        Clone the production table → sbx_{table}_{timestamp}
        │
 4. apply_transformation  Run your SQL on the sandbox (never production)
        │
 5. save_quality_checks   Write check SQL files to quality_checks/runs/
        │
 6. run_quality_checks    Execute checks: sandbox vs production
        │
 7. raise_pr              Push branch → open GitHub PR for your review
```

---

### Step 1 — Start migration

Claude creates a local git branch (`migration/{description}-{timestamp}`) to track every file written during this session. All subsequent commits land on this branch.

**What gets created:**
- A new git branch cut from your default branch (`main` / `master`)

**What Claude tells you:**
```
✅ Migration branch created | Branch: migration/apply-10pct-markup-20260406-1030 | Cut from: master | Repo root: /your/project | Working dir: clean
```

---

### Step 2 — Explore your data

Claude reads your production tables before touching anything. All three tools are read-only — no write can slip through.

| Tool | What it does |
|---|---|
| `list_tables` | Lists all production tables (sandbox `sbx_*` tables filtered out) |
| `describe_table` | Returns column names, types, and row count |
| `query_table` | Runs a SELECT — any non-SELECT statement is hard-blocked |

---

### Step 3 — Create a sandbox

Claude makes a full copy of the target table in the same schema with an `sbx_` prefix. Your production table is not touched at all from this point forward.

**What gets created:**
- `{schema}.sbx_{table}_{YYYYMMDD_HHMM}` — exact copy of the production table

**What Claude tells you:**
```
✅ Sandbox created | Production: analytics.orders | Sandbox: analytics.sbx_orders_20260406_1030 | Rows copied: 50,000 | Columns: order_id (BIGINT), amount (DOUBLE), status (VARCHAR)…
```

**Safety:** a two-layer production guard activates here:
- **SQL inspection** — every subsequent SQL statement is parsed by `sqlglot`. Any write targeting a non-`sbx_*` table raises a hard error, even if the SQL is valid.
- **Databricks dual-token mode (optional)** — a separate write-only credential scoped to `sbx_*` tables replaces the production credential entirely.

---

### Step 4 — Apply the transformation

Claude rewrites your SQL to target the sandbox table and executes it. The production-equivalent SQL (with real table names) is saved to a migration file and committed.

**What gets written:**
- `migrations/{timestamp}-{table}.sql` — the SQL rewritten for production, committed to your branch

**What Claude tells you:**
```
✅ Transformation #1 applied to 'sbx_orders_20260406_1030' | File: migrations/2026-04-06-103045-orders.sql | Commit: a3f9c12 "migration(orders): add transformation SQL" | SQL: UPDATE analytics.orders SET amount = ROUND(amount * 1.10, 2) WHERE status = 'completed';
```

You can call `preview_transformation` at any point to inspect sandbox data with a SELECT before or after applying a change.

---

### Step 5 — Save quality checks

Claude reads the check library in `quality_checks/`, picks the checks relevant to your transformation, fills in real table and column names, and writes them as SQL files.

**What gets written:**
- `quality_checks/runs/{sandbox_name}/{check_name}.sql` — one file per check, committed to your branch

**What Claude tells you:**
```
✅ 3 quality checks saved for 'sbx_orders_20260406_1030' | Dir: quality_checks/runs/sbx_orders_20260406_1030 | Commit: b7d2e45 | Checks: row_count_match.sql — row counts match · sum_comparison.sql — total amount unchanged · null_check.sql — no NULLs introduced
```

**Check library categories:**

| Category | Checks available |
|---|---|
| Volume | Row count match, new rows inspection |
| Numeric | Mean drift, sum comparison, min/max bounds |
| Categorical | Value set drift, distribution shift |
| Integrity | Uniqueness on keys, foreign key integrity, referential completeness |

Each check returns either a `status` column (`PASS` / `FAIL`) or violation rows (0 rows = PASS).

---

### Step 6 — Run quality checks

Claude executes every saved check against the sandbox, compares to production, and records the results. Two files are written and committed.

**What gets written:**
- `context/{table}.md` — auto-generated table description with column notes (human-editable, preserved across runs)
- `MIGRATION_NOTES.md` — full migration report: branch, SQL applied, every check result

**What Claude tells you:**
```
Quality Check Results — sbx_orders_20260406_1030 | Run: abc-123 | row_count_match: ✅ PASS · null_check: ✅ PASS · sum_comparison: ❌ FAIL (prod_sum=50000 sbx_sum=55000) | ❌ 1 of 3 checks failed | Files written: context/orders.md (a1b2c3d) · MIGRATION_NOTES.md (e4f5g6h)
```

Quality checks are advisory — you decide whether to proceed. The PR can be raised regardless of pass/fail.

---

### Step 7 — Raise a PR

Claude pushes the migration branch and opens a GitHub PR. The PR body is populated from `MIGRATION_NOTES.md`, so reviewers see the full context: branch, SQL, and quality check results.

**What gets pushed:**
```
migrations/2026-04-06-103045-orders.sql
quality_checks/runs/sbx_orders_20260406_1030/row_count_match.sql
quality_checks/runs/sbx_orders_20260406_1030/null_check.sql
quality_checks/runs/sbx_orders_20260406_1030/sum_comparison.sql
context/orders.md
MIGRATION_NOTES.md
```

**What Claude tells you:**
```
✅ PR raised | Branch: migration/apply-10pct-markup-20260406-1030 → master | [Migration] Apply 10% markup to completed orders | PR URL: https://github.com/your-org/your-repo/pull/42 | Files in PR: migrations/… · quality_checks/… · context/orders.md · MIGRATION_NOTES.md
```

You review the PR, inspect the migration SQL and quality report, and merge when ready. Only the `migrations/` SQL runs against production — Prelight never executes anything on production directly.

---

## Try It Right Now

You've got an `orders` and `customers` table ready to go. Here's a full end-to-end flow you can run immediately.

> **Note:** Prelight writes migration files directly into your git repo as you work. Make sure Claude Code is opened from inside a git-tracked directory when working with your own data.

### Explore — understand what's there before you change anything

```
List my tables using prelight
```
```
Describe the orders table
```
```
Show me 10 rows from orders where status is completed
```

### Transform — make changes safely, sandbox first

```
Add a region column to orders and set every row to EMEA
```
```
Cancel all pending orders older than 30 days
```
```
Apply a 10% markup to completed orders
```

Claude will sandbox it, show you what changed, and wait for your go-ahead.

### Quality checks — verify nothing broke before you ship

```
Run quality checks
```

Claude picks the right checks, runs them against sandbox vs production, and gives you a clear report.

### Ship — push the branch and open a PR

When quality checks pass, Claude will tell you the branch is ready. Say:

```
Raise a PR — Add region column to orders for EMEA reporting
```

Claude pushes the branch and gives you a direct link to open the PR on GitHub.

### Go bigger — multi-table changes in one branch

```
Apply a 10% discount to orders over $500 and mark those customers as premium.
Raise one PR for both changes.
```

---

## Try to Break It

Prelight blocks any direct write to production. Go ahead and try:

```
UPDATE analytics.orders SET status = 'archived' WHERE order_id = 1001
```

Claude will refuse and walk you through the safe path instead.

---

## Ready to Use Your Own Data?

Done with the demo? Point Prelight at your own data in seconds — no restart needed.

### Use your own DuckDB file

Tell Claude: `Use my DuckDB at /path/to/your/file.duckdb`

### Use Databricks

Tell Claude: `Switch to Databricks` — Claude will ask for your workspace URL, HTTP path, and access token.

### Raise PRs

Make sure your project directory is a git repo with a remote configured. When you say "raise a PR", Claude pushes the migration branch and gives you a direct link to open it on GitHub — no token setup required.

---

## Supported Clients

| Client | Status |
|---|---|
| Claude Desktop | Available now |
| Claude Code | Available now |
| Gemini CLI | Coming soon |
| Codex CLI | Coming soon |

---

## Troubleshooting

| Problem | Solution |
|---|---|
| Tools don't appear | Claude Desktop: fully quit and reopen. Claude Code: start a new session |
| `config.yaml not found` | Set `PRELIGHT_CONFIG=/absolute/path/to/config.yaml` in your Claude config |
| `uv: command not found` after install | Open a new terminal and re-run the setup command |
| Python version error | Ensure Python 3.11+ is installed: `python3 --version` |
| DuckDB file not found | Re-run setup or check that `~/.prelight/prelight.duckdb` exists |
| Databricks connection failed | Verify `host` starts with `https://`, token is valid, warehouse is running |
| Push fails on raise_pr | Verify the repo has a remote configured: `git remote -v` |
