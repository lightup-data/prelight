#!/usr/bin/env bash
set -e

case "$1" in
  mcp-server)
    # Used when Claude Desktop connects via docker run -i
    exec uv run --directory /app prelight
    ;;
  setup-demo)
    # Used once during setup.sh --docker to load demo data
    shift
    exec uv run --directory /app python /app/setup/duckdb/init_local.py "$@"
    ;;
  *)
    # Default: start an interactive Claude Code session.
    # Volume mounts are already in place at this point.

    # Build /root/.claude.json:
    # - if host file exists: copy auth from it, strip host mcpServers
    # - always inject prelight as the only MCP server
    # Claude Code reads MCP servers from .claude.json, not settings.json.
    python3 -c "
import json, os
src = '/root/.claude.json.host'
data = json.load(open(src)) if os.path.exists(src) else {}
data['mcpServers'] = {
    'prelight': {
        'type': 'stdio',
        'command': 'uv',
        'args': ['--directory', '/app', 'run', 'prelight'],
        'env': {'PRELIGHT_CONFIG': os.environ.get('PRELIGHT_CONFIG', '')}
    }
}
json.dump(data, open('/root/.claude.json', 'w'), indent=2)
"
    exec claude "$@"
    ;;
esac
