#!/usr/bin/env bash
# Layer 3 Enforcement Hook — PreToolUse / Bash
# Runs before any Bash tool call. Warns when an agent is about to commit
# directly to main — a violation of the branching strategy in CLAUDE.md.
#
# Exit 0: allow the tool call to proceed (soft enforcement — warning only)
# Exit 2: block the tool call (not used here — reserved for hard enforcement)
#
# Exceptions allowed on main (per CLAUDE.md):
#   - claude_code_productivity.json commits (tracking file, not code)
#   - Changelog / productivity metric update commits

REPO_ROOT="/Users/clauseduardpetraeus/HealthReporting"

# Parse the command from the tool input
CMD=$(echo "$CLAUDE_TOOL_INPUT" \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('command',''))" 2>/dev/null \
    || echo "")

# Only check git commit commands
if echo "$CMD" | grep -q 'git commit'; then
    BRANCH=$(git -C "$REPO_ROOT" branch --show-current 2>/dev/null || echo "")

    if [ "$BRANCH" = "main" ]; then
        # Allow: productivity tracker and changelog-only commits
        if ! echo "$CMD" | grep -qE \
            '(claude_code_productivity|productivity metrics|Update productivity|chore.*productivity|chore: update productivity)'; then
            echo ""
            echo "GOVERNANCE WARNING: Direct commit to main detected."
            echo "Per CLAUDE.md branching strategy, all feature work must use branches."
            echo "  Allowed on main: claude_code_productivity.json updates"
            echo "  Required for all other changes: git checkout -b feature/<name>"
            echo ""
        fi
    fi
fi

# Always exit 0 — soft enforcement (warning, not blocking)
exit 0
