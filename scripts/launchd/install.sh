#!/usr/bin/env bash
# Install launchd plist files from templates.
#
# Expands ${DATA_LAKE_ROOT} and ${REPO_ROOT} placeholders using environment
# variables, then copies the result to ~/Library/LaunchAgents/.
#
# Usage:
#   DATA_LAKE_ROOT=/Users/Shared/data_lake REPO_ROOT=/path/to/HealthReporting \
#     bash scripts/launchd/install.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TARGET_DIR="$HOME/Library/LaunchAgents"

: "${DATA_LAKE_ROOT:?Set DATA_LAKE_ROOT (e.g. /Users/Shared/data_lake)}"
: "${REPO_ROOT:?Set REPO_ROOT (e.g. path to HealthReporting checkout)}"

mkdir -p "$TARGET_DIR"

for template in "$SCRIPT_DIR"/*.plist.template; do
    [ -f "$template" ] || continue
    plist_name="$(basename "${template%.template}")"
    dest="$TARGET_DIR/$plist_name"

    sed \
        -e "s|\${DATA_LAKE_ROOT}|${DATA_LAKE_ROOT}|g" \
        -e "s|\${REPO_ROOT}|${REPO_ROOT}|g" \
        "$template" > "$dest"

    echo "Installed: $dest"
done

echo ""
echo "Reload with:"
echo "  launchctl unload ~/Library/LaunchAgents/com.health.*.plist 2>/dev/null; \\"
echo "  launchctl load ~/Library/LaunchAgents/com.health.*.plist"
