#!/usr/bin/env bash
# Layer 3 Enforcement Hook — PostToolUse / Bash
# Runs automatically after any Bash tool call that contains 'git commit'.
# Purpose: ensure productivity tracker is always updated after commits,
#          even if the agent forgets to run it manually.
#
# Claude Code settings.json calls this script as a PostToolUse hook.
# CLAUDE_TOOL_INPUT is set by Claude Code (JSON with 'command' field).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

# Parse the command from the tool input
CMD=$(echo "$CLAUDE_TOOL_INPUT" \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('command',''))" 2>/dev/null \
    || echo "")

# Only act on git commit commands — skip if it's already the productivity tracker
if echo "$CMD" | grep -q 'git commit' && ! echo "$CMD" | grep -q 'update_productivity'; then
    cd "$REPO_ROOT"
    if [ -f ".venv/bin/python" ] && [ -f "update_productivity.py" ]; then
        .venv/bin/python update_productivity.py 2>/dev/null \
            && echo "[GOVERNANCE] Productivity tracker updated after commit." \
            || true
    fi
fi
