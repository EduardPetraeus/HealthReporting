#!/usr/bin/env bash
# Convenience launcher for the Health Chat API + Web UI.
#
# Usage:
#   export HEALTH_ENV=dev
#   ./run_chat.sh
#
# ANTHROPIC_API_KEY and HEALTH_API_TOKEN are loaded from macOS Keychain automatically.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Activate venv if present
if [ -f "$SCRIPT_DIR/.venv/bin/activate" ]; then
    source "$SCRIPT_DIR/.venv/bin/activate"
fi

# Set PYTHONPATH to the platform root so imports resolve
export PYTHONPATH="${SCRIPT_DIR}/health_unified_platform:${PYTHONPATH:-}"

# Default environment
export HEALTH_ENV="${HEALTH_ENV:-dev}"

echo "Starting Health Chat API (env=$HEALTH_ENV)..."
echo "Open http://127.0.0.1:8000 in your browser."
exec uvicorn health_platform.api.server:app --host 127.0.0.1 --port 8000
