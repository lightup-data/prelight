#!/usr/bin/env bash
# =============================================================================
#  Prelight — One-command setup
#
#  Standard install (Claude Desktop + Claude Code):
#    curl -sL https://raw.githubusercontent.com/lightup-data/prelight/master/setup.sh | bash
#
#  Docker install (isolated session — Claude Code only):
#    curl -sL https://raw.githubusercontent.com/lightup-data/prelight/master/setup-docker.sh | bash
#
#  Standard install:
#    1. Installs uv (Python package manager) if not already present
#    2. Clones the Prelight repo to ~/.prelight  (or updates it if already there)
#    3. Installs all Python dependencies
#    4. Creates config.yaml with a local DuckDB database (zero infra needed)
#    5. Creates the DuckDB file and loads demo data (orders + customers)
#    6. Registers Prelight with Claude Desktop and Claude Code CLI
#
#  Docker install (--docker flag):
#    1. Clones the repo and builds a Docker image
#    2. Creates config.yaml and loads demo data via the container
#    3. Installs a prelight-session launcher at ~/.local/bin/prelight-session
#    4. Registers Prelight with Claude Desktop (docker run -i mode)
#    After install: cd into your project and run  prelight-session
#
#  To use Databricks instead of DuckDB, type in Claude:
#    "Switch to Databricks"  — Claude will ask for your credentials.
# =============================================================================

set -euo pipefail

REPO_URL="https://github.com/lightup-data/prelight.git"
INSTALL_DIR="${PRELIGHT_INSTALL_DIR:-$HOME/.prelight}"
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

# ── Docker mode ───────────────────────────────────────────────────────────────
DOCKER_MODE=false
for arg in "$@"; do [[ "$arg" == "--docker" ]] && DOCKER_MODE=true; done

if $DOCKER_MODE; then

  # 1. Docker must be installed and running
  if ! command -v docker &>/dev/null; then
    echo "Error: Docker is required for --docker install. Get it at https://docker.com"
    exit 1
  fi
  if ! docker info &>/dev/null 2>&1; then
    echo "Error: Docker daemon is not running. Start Docker Desktop and re-run."
    exit 1
  fi
  success "Docker found"

  # 2. git is still needed to clone the repo for the image build
  if ! command -v git &>/dev/null; then
    echo "Error: git is required. Install from https://git-scm.com and re-run."
    exit 1
  fi

  # 3. Clone or update the repo (source is baked into the image)
  #    Skipped when PRELIGHT_INSTALL_DIR is set (local dev/testing).
  if [ -n "${PRELIGHT_INSTALL_DIR:-}" ]; then
    success "Using local repo at $INSTALL_DIR"
  elif [ -d "$INSTALL_DIR/.git" ]; then
    info "Updating Prelight..."
    git -C "$INSTALL_DIR" pull --ff-only --quiet
    success "Prelight updated"
  else
    info "Downloading Prelight to $INSTALL_DIR..."
    git clone --quiet "$REPO_URL" "$INSTALL_DIR"
    success "Prelight downloaded"
  fi

  # 4. Build the Docker image
  info "Building Docker image (3–5 min on first run, faster after)..."
  BUILD_LOG=$(mktemp)
  docker build -t prelight/session "$INSTALL_DIR" > "$BUILD_LOG" 2>&1
  BUILD_STATUS=$?
  grep -E "^Step|^Successfully|^ERROR" "$BUILD_LOG" || true
  rm -f "$BUILD_LOG"
  if [ "$BUILD_STATUS" -ne 0 ]; then
    echo "Error: Docker build failed. Run this to see full output:"
    echo "  docker build -t prelight/session $INSTALL_DIR"
    exit 1
  fi
  success "Docker image ready"

  # 5. Write config.yaml (same as standard path — written on the host)
  CONFIG_PATH="$INSTALL_DIR/config.yaml"
  info "Writing configuration file..."
  python3 - <<PYCONFIG 2>/dev/null
from pathlib import Path

config_path = Path("""$CONFIG_PATH""")
db_path     = """$DB_PATH"""
schema      = """$SCHEMA"""

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
PYCONFIG
  success "Configuration file saved to $CONFIG_PATH"

  # 6. Load demo data via the container
  info "Loading demo data..."
  docker run --rm \
    --workdir /app \
    -v "$INSTALL_DIR:$INSTALL_DIR" \
    -e PRELIGHT_CONFIG="$CONFIG_PATH" \
    prelight/session setup-demo \
      --path "$DB_PATH" \
      --schema "$SCHEMA" > /dev/null 2>&1
  success "Demo data ready (schema: $SCHEMA, tables: orders, customers)"

  # 7. Install the prelight-session launcher
  LAUNCHER_DIR="$HOME/.local/bin"
  LAUNCHER="$LAUNCHER_DIR/prelight-session"
  mkdir -p "$LAUNCHER_DIR"
  cat > "$LAUNCHER" << 'LAUNCHER_SCRIPT'
#!/usr/bin/env bash
# prelight-session — run Claude Code with prelight isolated inside Docker.
# Run this from inside the git repo you want to work in.
DOCKER_ARGS=(-it --rm)
DOCKER_ARGS+=(-v "$HOME/.prelight:$HOME/.prelight")
DOCKER_ARGS+=(-e "PRELIGHT_CONFIG=$HOME/.prelight/config.yaml")
DOCKER_ARGS+=(-v "$HOME/.claude:/root/.claude")
DOCKER_ARGS+=(-v "$(pwd):/workspace")
[ -f "$HOME/.claude.json" ] && DOCKER_ARGS+=(-v "$HOME/.claude.json:/root/.claude.json.host:ro")
[ -f "$HOME/.gitconfig" ]   && DOCKER_ARGS+=(-v "$HOME/.gitconfig:/root/.gitconfig:ro")
[ -d "$HOME/.ssh" ]         && DOCKER_ARGS+=(-v "$HOME/.ssh:/root/.ssh:ro")
[ -d "$HOME/.config/gh" ]   && DOCKER_ARGS+=(-v "$HOME/.config/gh:/root/.config/gh:ro")
docker run "${DOCKER_ARGS[@]}" prelight/session "$@"
LAUNCHER_SCRIPT
  chmod +x "$LAUNCHER"
  success "prelight-session installed at $LAUNCHER"

  if [[ ":$PATH:" != *":$LAUNCHER_DIR:"* ]]; then
    warn "$LAUNCHER_DIR is not in your PATH. Add to your shell profile:"
    warn "  export PATH=\"\$HOME/.local/bin:\$PATH\""
  fi

  # 8. Register with Claude Desktop (docker run -i MCP server mode)
  info "Registering with Claude Desktop..."
  python3 - <<PYDESKTOP 2>/dev/null
import json, platform, os
from pathlib import Path

system = platform.system()
if system == "Darwin":
    cfg_path = Path.home() / "Library" / "Application Support" / "Claude" / "claude_desktop_config.json"
elif system == "Windows":
    appdata = os.environ.get("APPDATA") or str(Path.home() / "AppData" / "Roaming")
    cfg_path = Path(appdata) / "Claude" / "claude_desktop_config.json"
else:
    xdg = os.environ.get("XDG_CONFIG_HOME") or str(Path.home() / ".config")
    cfg_path = Path(xdg) / "Claude" / "claude_desktop_config.json"

home         = str(Path.home())
prelight_dir = f"{home}/.prelight"
config_yaml  = f"{home}/.prelight/config.yaml"

try:
    raw = cfg_path.read_text(encoding="utf-8").strip() if cfg_path.exists() else ""
    existing = json.loads(raw) if raw else {}
except Exception:
    existing = {}

existing.setdefault("mcpServers", {})
existing["mcpServers"]["prelight"] = {
    "command": "docker",
    "args": [
        "run", "-i", "--rm",
        "-v", f"{prelight_dir}:{prelight_dir}",
        "-e", f"PRELIGHT_CONFIG={config_yaml}",
        "prelight/session",
        "mcp-server",
    ],
}

cfg_path.parent.mkdir(parents=True, exist_ok=True)
cfg_path.write_text(json.dumps(existing, indent=2) + "\n", encoding="utf-8")
PYDESKTOP
  success "Claude Desktop registered"

  # 9. Done
  echo ""
  echo -e "${GREEN}Prelight (Docker) installed.${RESET}"
  echo ""
  echo -e "${BOLD}cd into your project and run:${RESET}"
  echo ""
  echo -e "  ${BOLD}prelight-session${RESET}"
  echo ""
  exit 0
fi

# ── 1. Check git ──────────────────────────────────────────────────────────────
if ! command -v git &>/dev/null; then
    echo "Error: git is required. Install from https://git-scm.com and re-run."
    exit 1
fi

# ── 2. Install uv ─────────────────────────────────────────────────────────────
if ! command -v uv &>/dev/null; then
    info "Installing uv (Python package manager)..."
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
    success "uv installed"
fi

# ── 3. Clone or update the repo ───────────────────────────────────────────────
if [ -d "$INSTALL_DIR/.git" ]; then
    info "Updating Prelight..."
    git -C "$INSTALL_DIR" pull --ff-only --quiet
    success "Prelight updated"
else
    info "Downloading Prelight to $INSTALL_DIR..."
    git clone --quiet "$REPO_URL" "$INSTALL_DIR"
    success "Prelight downloaded"
fi

cd "$INSTALL_DIR"

# ── 4. Install Python dependencies ───────────────────────────────────────────
info "Installing Python dependencies..."
uv sync --quiet
success "Dependencies ready"

# ── 5. Write config.yaml (create if missing, update path if exists) ────────────
CONFIG_PATH="$INSTALL_DIR/config.yaml"

info "Writing configuration file (database path, schema, credentials)..."
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
success "Configuration file saved to $CONFIG_PATH"

# ── 6. Load demo data ─────────────────────────────────────────────────────────
info "Installing DuckDB and loading demo data at $DB_PATH..."
PRELIGHT_CONFIG="$CONFIG_PATH" uv run python setup/duckdb/init_local.py \
    --path "$DB_PATH" \
    --schema "$SCHEMA" >/dev/null 2>&1
success "Demo data ready (schema: $SCHEMA, tables: orders, customers)"

# ── 7. Register with Claude Desktop & Claude Code ─────────────────────────────
info "Registering the Prelight MCP server with Claude..."
PRELIGHT_CONFIG="$CONFIG_PATH" uv run prelight install >/dev/null 2>&1
success "MCP server registered"

# ── Done ──────────────────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}Prelight installed.${RESET}"
echo ""
echo -e "${CYAN}  Tip: Open Claude Code from inside a git repo when working with your own data."
echo -e "  Prelight writes migration files and notes directly into your repo as you work.${RESET}"
echo ""
echo -e "${BOLD}Next steps:${RESET}"
echo ""
echo "  Claude Desktop  →  Cmd+Q, reopen, start a new conversation"
echo "  Claude Code     →  open a new session"
echo ""
echo -e "  Then type: ${BOLD}List my tables using prelight${RESET}"
echo ""
