"""
prelight install

If config.yaml already exists:
  → Skips all prompts, injects the MCP server entry into Claude Desktop's
    claude_desktop_config.json and exits. Zero interaction required.

If config.yaml does not exist:
  1. Asks which backend to use (Databricks / DuckDB local / DuckDB existing file).
  2. Collects credentials for the chosen backend.
  3. Prompts for GitHub credentials (optional — skip with Enter).
  4. Writes config.yaml.
  5. Injects the Claude Desktop / Claude Code MCP entry.
"""

from __future__ import annotations

import configparser
import json
import os
import platform
import sys
from pathlib import Path


def _mask(value: str, visible: int = 4) -> str:
    """Return a partially-masked string for safe display (e.g. dapi****)."""
    if not value:
        return ""
    if len(value) <= visible:
        return "****"
    return value[:visible] + "****"


def _prompt(label: str, default: str = "", display_default: str | None = None) -> str:
    """
    Print a prompt and read one line. Press Enter to accept *default*.
    *display_default* is shown in brackets instead of *default* when provided
    (useful for masking tokens while still accepting the real value on Enter).
    """
    shown = display_default if display_default is not None else default
    suffix = f" [{shown}]" if shown else ""
    try:
        val = input(f"  {label}{suffix}: ").strip()
    except (KeyboardInterrupt, EOFError):
        print("\nAborted.")
        sys.exit(1)
    return val if val else default


def _prompt_yn(label: str, default_yes: bool = True) -> bool:
    hint = "Y/n" if default_yes else "y/N"
    val = _prompt(f"{label} [{hint}]").lower()
    if not val:
        return default_yes
    return val.startswith("y")


def _prompt_choice(label: str, options: list[str]) -> int:
    """Present a numbered menu and return the 0-based index of the chosen option."""
    print(f"\n  {label}")
    for i, opt in enumerate(options, 1):
        print(f"    {i}. {opt}")
    while True:
        raw = _prompt(f"Enter number (1–{len(options)})")
        if raw.isdigit() and 1 <= int(raw) <= len(options):
            return int(raw) - 1
        print(f"    Please enter a number between 1 and {len(options)}.")


# ── Databricks CLI config ─────────────────────────────────────────────────────


def _parse_databrickscfg() -> dict[str, dict[str, str]]:
    """
    Parse ~/.databrickscfg and return {profile_name: {field: value}}.
    """
    cfg_path = Path.home() / ".databrickscfg"
    if not cfg_path.exists():
        return {}

    parser = configparser.ConfigParser()
    parser.read(cfg_path)

    profiles: dict[str, dict[str, str]] = {}

    defaults = dict(parser.defaults())
    if defaults.get("host") or defaults.get("token"):
        profiles["DEFAULT"] = defaults

    for section in parser.sections():
        profiles[section] = dict(parser[section])

    return profiles


def _normalize_host(host: str) -> str:
    host = host.strip()
    if host and not host.startswith("https://"):
        host = "https://" + host
    return host.rstrip("/")


# ── Project / binary paths ────────────────────────────────────────────────────


def _get_project_root() -> Path:
    bin_dir = Path(sys.executable).parent
    if bin_dir.name in ("bin", "Scripts"):
        venv_dir = bin_dir.parent
        if venv_dir.name in (".venv", "venv", "env", ".env"):
            return venv_dir.parent
    return Path.cwd()


def _get_command_path() -> Path:
    bin_dir = Path(sys.executable).parent

    candidate = bin_dir / "prelight"
    if candidate.exists():
        return candidate

    candidate_win = bin_dir / "prelight.exe"
    if candidate_win.exists():
        return candidate_win

    argv0 = Path(sys.argv[0]).resolve()
    return argv0


# ── Claude Desktop config ─────────────────────────────────────────────────────


def _find_claude_desktop_config() -> Path:
    system = platform.system()
    if system == "Darwin":
        return (
            Path.home()
            / "Library"
            / "Application Support"
            / "Claude"
            / "claude_desktop_config.json"
        )
    if system == "Windows":
        appdata = os.environ.get("APPDATA") or str(Path.home() / "AppData" / "Roaming")
        return Path(appdata) / "Claude" / "claude_desktop_config.json"
    xdg = os.environ.get("XDG_CONFIG_HOME") or str(Path.home() / ".config")
    return Path(xdg) / "Claude" / "claude_desktop_config.json"


def _load_claude_config(path: Path) -> dict:
    if not path.exists():
        return {}

    text = path.read_text(encoding="utf-8")
    if not text.strip():
        return {}

    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        backup = path.with_suffix(".json.bak")
        backup.write_bytes(path.read_bytes())
        print(
            f"\n  Warning: {path} contains invalid JSON ({exc}).\n"
            f"  A backup was saved to {backup}.\n"
            f"  The file will be repaired with the existing servers preserved where possible."
        )
        return {}


def _register_claude_code(command: Path, config_yaml: Path) -> None:
    """Register the MCP server with Claude Code CLI using `claude mcp add`."""
    import shutil
    import subprocess

    if not shutil.which("claude"):
        print("  Claude Code (CLI) not found — skipping.")
        return

    subprocess.run(
        ["claude", "mcp", "remove", "prelight", "--scope", "user"],
        capture_output=True,
    )

    result = subprocess.run(
        [
            "claude", "mcp", "add", "prelight",
            str(command),
            "--scope", "user",
            "-e", f"PRELIGHT_CONFIG={config_yaml}",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        print("  Claude Code: prelight registered via `claude mcp add`")
    else:
        print(f"  Claude Code: registration failed — {result.stderr.strip()}")
        print(
            f"  Run manually:  claude mcp add prelight {command} "
            f"--scope user -e PRELIGHT_CONFIG={config_yaml}"
        )


def _inject_mcp_server(existing: dict, command: Path, config_yaml: Path) -> dict:
    if "mcpServers" not in existing:
        existing["mcpServers"] = {}

    existing["mcpServers"]["prelight"] = {
        "command": str(command),
        "env": {
            "PRELIGHT_CONFIG": str(config_yaml),
        },
    }
    return existing


# ── Backend-specific config wizards ───────────────────────────────────────────


def _collect_databricks_config() -> str:
    """Interactive wizard for Databricks credentials. Returns the databricks: YAML block."""

    # Detect ~/.databrickscfg
    prefilled_host = ""
    prefilled_token = ""
    prefilled_http_path = ""

    profiles = _parse_databrickscfg()

    if profiles:
        profile_names = list(profiles.keys())
        print(f"\n  Detected ~/.databrickscfg ({len(profiles)} profile(s) found).\n")

        chosen_data: dict[str, str] | None = None

        if len(profiles) == 1:
            name, data = next(iter(profiles.items()))
            host_display = data.get("host", "(no host)")
            print(f"  Profile [{name}]: host={host_display}")
            if _prompt_yn("  Use this profile for Databricks credentials?"):
                chosen_data = data
        else:
            print("  Available profiles:")
            for i, name in enumerate(profile_names, 1):
                host_display = profiles[name].get("host", "(no host)")
                print(f"    {i}. [{name}]: host={host_display}")
            choice = _prompt(
                f"Choose a profile number (1–{len(profile_names)}) or press Enter to skip"
            )
            if choice.isdigit() and 1 <= int(choice) <= len(profile_names):
                chosen_name = profile_names[int(choice) - 1]
                chosen_data = profiles[chosen_name]
                print(f"  Using profile [{chosen_name}].")

        if chosen_data:
            prefilled_host = _normalize_host(chosen_data.get("host", ""))
            prefilled_token = chosen_data.get("token", "")
            prefilled_http_path = chosen_data.get("http_path", "")
    else:
        print("\n  No ~/.databrickscfg found — enter credentials manually.")

    print("\n── Databricks Connection ─────────────────────────────────────────")

    host = ""
    while not host:
        host = _prompt("Workspace URL", prefilled_host)
        if host:
            host = _normalize_host(host)
        if not host:
            print("    Host is required.")

    http_path = ""
    while not http_path:
        http_path = _prompt(
            "SQL Warehouse HTTP path  (Warehouse > Connection Details > HTTP Path)",
            prefilled_http_path,
        )
        if not http_path:
            print("    HTTP path is required.")

    schema = _prompt("Databricks schema", "analytics")

    print("\n── Databricks Tokens ─────────────────────────────────────────────")
    print("  Dual-token mode (recommended): one read-only token for production,")
    print("  one write-enabled token scoped to sandbox tables only.")
    print("  Single-token mode: one PAT used for both (simpler, less isolated).")
    print()
    use_dual = _prompt_yn("  Use dual-token mode?", default_yes=True)

    if use_dual:
        prod_token = ""
        while not prod_token:
            prod_token = _prompt(
                "Production token  (read-only PAT for production tables)",
                prefilled_token,
                display_default=_mask(prefilled_token) if prefilled_token else None,
            )
            if not prod_token:
                print("    Production token is required.")

        sandbox_token = ""
        while not sandbox_token:
            sandbox_token = _prompt(
                "Sandbox token  (write-enabled PAT for sandbox tables only)",
            )
            if not sandbox_token:
                print("    Sandbox token is required.")

        token_block = (
            f'  prod_token: "{prod_token}"\n'
            f'  sandbox_token: "{sandbox_token}"\n'
        )
    else:
        single_token = ""
        while not single_token:
            single_token = _prompt(
                "Personal access token",
                prefilled_token,
                display_default=_mask(prefilled_token) if prefilled_token else None,
            )
            if not single_token:
                print("    Token is required.")
        token_block = f'  token: "{single_token}"\n'

    return (
        f'databricks:\n'
        f'  host: "{host}"\n'
        + token_block +
        f'  http_path: "{http_path}"\n'
        f'  schema: "{schema}"\n'
        f'  sandbox_prefix: "sbx_"\n'
        f'  audit_table: "qg_quality_results"\n'
    )


def _collect_duckdb_config(existing_file: bool) -> str:
    """Interactive wizard for DuckDB config. Returns the duckdb: YAML block."""
    print("\n── DuckDB Database ───────────────────────────────────────────────")

    if existing_file:
        print("  Enter the path to your existing .duckdb file.")
        db_path = ""
        while not db_path:
            db_path = _prompt("Path to .duckdb file  (absolute or relative)")
            if not db_path:
                print("    Path is required.")
    else:
        print("  A new DuckDB file will be created at the path you specify.")
        print("  Press Enter to use the default (./prelight.duckdb).")
        db_path = _prompt("Path to .duckdb file", "./prelight.duckdb")

    schema = _prompt("Schema name", "analytics")

    path_line = f'  path: "{db_path}"\n' if db_path else ""
    return (
        f'duckdb:\n'
        + path_line +
        f'  schema: "{schema}"\n'
        f'  sandbox_prefix: "sbx_"\n'
        f'  audit_table: "qg_quality_results"\n'
    )


# ── Wizard ────────────────────────────────────────────────────────────────────


def run_install() -> None:  # noqa: C901
    print("\n=== Prelight — Setup ===\n")

    project_root = _get_project_root()
    config_path = project_root / "config.yaml"
    command_path = _get_command_path()
    claude_cfg_path = _find_claude_desktop_config()

    # ── Fast path: config.yaml already exists ─────────────────────────────────
    if config_path.exists():
        print(f"Found config.yaml at {config_path} — skipping credential prompts.")
        print()
    else:
        # ── Full wizard: config.yaml missing ──────────────────────────────────

        # 1. Backend selection
        backend_idx = _prompt_choice(
            "Which database backend would you like to use?",
            [
                "Databricks  (cloud data warehouse — bring your own workspace)",
                "DuckDB — new local file  (zero infra, runs on your laptop)",
                "DuckDB — existing file  (bring your own .duckdb file)",
            ],
        )

        # 2. Collect backend config
        if backend_idx == 0:
            backend_block = _collect_databricks_config()
            is_duckdb = False
        elif backend_idx == 1:
            backend_block = _collect_duckdb_config(existing_file=False)
            is_duckdb = True
        else:
            backend_block = _collect_duckdb_config(existing_file=True)
            is_duckdb = True

        print()

        # 3. Write config.yaml
        config_content = backend_block + "\n" + "quality:\n  row_count_drift_pct: 5\n"
        config_path.write_text(config_content, encoding="utf-8")
        print(f"  config.yaml written to {config_path}")

        # 5. Offer to run demo data setup for DuckDB immediately
        if is_duckdb:
            print()
            if _prompt_yn("  Load demo data now (orders + customers tables)?", default_yes=True):
                print()
                from setup.duckdb.init_local import run_init
                import yaml
                raw = yaml.safe_load(config_path.read_text())
                db_cfg = raw.get("duckdb", {})
                run_init(db_path=db_cfg.get("path"), schema=db_cfg.get("schema", "analytics"))

    # ── Inject Claude Desktop config ───────────────────────────────────────
    existing = _load_claude_config(claude_cfg_path)
    updated = _inject_mcp_server(existing, command_path, config_path)

    claude_cfg_path.parent.mkdir(parents=True, exist_ok=True)
    claude_cfg_path.write_text(
        json.dumps(updated, indent=2) + "\n",
        encoding="utf-8",
    )

    other_servers = [k for k in updated.get("mcpServers", {}) if k != "prelight"]
    preserved_note = (
        f" (preserved {len(other_servers)} existing server(s): {', '.join(other_servers)})"
        if other_servers
        else ""
    )
    print(f"  claude_desktop_config.json updated{preserved_note}")
    print(f"  Location: {claude_cfg_path}")

    # ── Register with Claude Code CLI ─────────────────────────────────────────
    _register_claude_code(command_path, config_path)

    # ── Done ──────────────────────────────────────────────────────────────────
    print("\n=== Setup complete ===\n")

    try:
        import yaml
        raw = yaml.safe_load(config_path.read_text())
        backend = "duckdb" if "duckdb" in raw else "databricks"
    except Exception:
        backend = "databricks"

    print("  Next steps:")
    if backend == "duckdb":
        print("    1. Start chatting — demo data is ready!")
    else:
        print("    1. Load demo data:  uv run prelight setup-demo")

    print()
    print("  Claude Desktop:")
    print("    • Fully quit (Cmd+Q) → reopen → new conversation")
    print("    • Type: List my tables")
    print()
    print("  Claude Code (CLI):")
    print("    • Start a new claude session → Type: List my tables")
    print()
    print("  Switch backends at any time by telling Claude:")
    print("    • \"Switch to Databricks\"  or  \"Use my DuckDB at /path/to/file\"")
    print()
