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

Type: **List my tables**

---

## What Claude Does So You Don't Have To

**1. Creates a sandbox** — Before touching anything, Claude makes a full copy of the table. Your production data stays exactly as it is.

**2. Applies the change** — Claude transforms the sandbox. You can inspect it, query it, compare it to production — nothing is locked in yet.

**3. Runs quality checks** — Claude checks for row count drift, numeric drift, referential integrity, and value distribution — sandbox vs production — and tells you exactly what changed.

**4. Commits to a branch** — Every change is committed to a local git branch as you go — migration SQL, quality check files, and notes. Say "raise a PR" when you're ready and Claude pushes the branch for you.

---

## Try It Right Now

You've got an `orders` and `customers` table ready to go. Here's a full end-to-end flow you can run immediately.

> **Note:** Prelight writes migration files directly into your git repo as you work. Make sure Claude Code is opened from inside a git-tracked directory when working with your own data.

### Explore — understand what's there before you change anything

```
List my tables
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
