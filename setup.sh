#!/usr/bin/env bash
# =============================================================================
#  Prelight — One-command setup
#  curl -sL https://raw.githubusercontent.com/prelight-data/prelight/main/setup.sh | bash
#
#  What this does:
#    1. Installs uv (Python package manager) if not already present
#    2. Clones the Prelight repo to ~/.prelight  (or updates it if already there)
#    3. Installs all Python dependencies
#    4. Creates config.yaml with a local DuckDB database (zero infra needed)
#    5. Creates the DuckDB file and loads demo data (orders + customers)
#    6. Prompts for GitHub token so you can raise PRs  (optional — skip with Enter)
#    7. Registers Prelight with Claude Desktop and Claude Code CLI
#
#  After this script finishes:
#    • Restart Claude Desktop (Cmd+Q, then reopen)
#    • Open a new conversation and type:  List my tables
#
#  To use Databricks instead of DuckDB, type in Claude:
#    "Switch to Databricks"  — Claude will ask for your credentials.
# =============================================================================

set -euo pipefail

REPO_URL="https://github.com/lightup-data/prelight.git"
INSTALL_DIR="$HOME/.prelight"
DB_PATH="$INSTALL_DIR/prelight.duckdb"
SCHEMA="analytics"

# ── Colours ───────────────────────────────────────────────────────────────────
BOLD="\033[1m"
GREEN="\033[0;32m"
YELLOW="\033[0;33m"
CYAN="\033[0;36m"
RESET="\033[0m"

info()    { echo -e "${CYAN}  →${RESET} $*"; }
success() { echo -e "${GREEN}  ✓${RESET} $*"; }
warn()    { echo -e "${YELLOW}  ⚠${RESET} $*"; }
header()  { echo -e "\n${BOLD}$*${RESET}"; }

echo ""
echo -e "${BOLD}Installing Prelight...${RESET}"
echo ""

# ── 1. Check git ──────────────────────────────────────────────────────────────
if ! command -v git &>/dev/null; then
    echo "Error: git is required. Install from https://git-scm.com and re-run."
    exit 1
fi

# ── 2. Install uv ─────────────────────────────────────────────────────────────
if ! command -v uv &>/dev/null; then
    curl -LsSf https://astral.sh/uv/install.sh | sh >/dev/null 2>&1
    if [ -f "$HOME/.local/bin/env" ]; then
        # shellcheck disable=SC1091
        source "$HOME/.local/bin/env" 2>/dev/null || true
    fi
    export PATH="$HOME/.local/bin:$PATH"
    if ! command -v uv &>/dev/null; then
        echo "Error: uv installed but not in PATH. Open a new terminal and re-run."
        exit 1
    fi
fi

# ── 3. Clone or update the repo ───────────────────────────────────────────────
if [ -d "$INSTALL_DIR/.git" ]; then
    git -C "$INSTALL_DIR" pull --ff-only --quiet
else
    git clone --quiet "$REPO_URL" "$INSTALL_DIR"
fi

cd "$INSTALL_DIR"

# ── 4. Install Python dependencies ───────────────────────────────────────────
uv sync --quiet

# ── 5. Write config.yaml (create if missing, update path if exists) ────────────
CONFIG_PATH="$INSTALL_DIR/config.yaml"

python3 - <<PYTHON 2>/dev/null
import sys
from pathlib import Path

config_path = Path("""$CONFIG_PATH""")
db_path = """$DB_PATH"""
schema = """$SCHEMA"""

if config_path.exists():
    try:
        import yaml
        raw = yaml.safe_load(config_path.read_text()) or {}
        if "duckdb" not in raw:
            raw["duckdb"] = {}
        raw["duckdb"]["path"] = db_path
        raw["duckdb"].setdefault("schema", schema)
        raw["duckdb"].setdefault("sandbox_prefix", "sbx_")
        raw["duckdb"].setdefault("audit_table", "qg_quality_results")
        config_path.write_text(yaml.dump(raw, default_flow_style=False, allow_unicode=True))
    except Exception:
        config_path.write_text(f"""duckdb:
  path: "{db_path}"
  schema: "{schema}"
  sandbox_prefix: "sbx_"
  audit_table: "qg_quality_results"

quality:
  row_count_drift_pct: 5
""")
else:
    config_path.write_text(f"""duckdb:
  path: "{db_path}"
  schema: "{schema}"
  sandbox_prefix: "sbx_"
  audit_table: "qg_quality_results"

quality:
  row_count_drift_pct: 5
""")
PYTHON

# ── 6. Load demo data ─────────────────────────────────────────────────────────
PRELIGHT_CONFIG="$CONFIG_PATH" uv run python setup/duckdb/init_local.py \
    --path "$DB_PATH" \
    --schema "$SCHEMA" >/dev/null 2>&1

# ── 7. Register with Claude Desktop & Claude Code ─────────────────────────────
PRELIGHT_CONFIG="$CONFIG_PATH" uv run prelight install >/dev/null 2>&1

# ── Done ──────────────────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}Prelight installed.${RESET}"
echo ""
echo -e "${BOLD}Next steps:${RESET}"
echo ""
echo "  Claude Desktop  →  Cmd+Q, reopen, start a new conversation"
echo "  Claude Code     →  open a new session"
echo ""
echo -e "  Then type: ${BOLD}List my tables${RESET}"
echo ""
