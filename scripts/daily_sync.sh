#!/bin/bash
# daily_sync.sh — Automated daily health data refresh
#
# Pipeline: Oura fetch → Bronze ingestion → Silver merge → Daily summary → Embedding
#
# Usage:
#   ./scripts/daily_sync.sh              (manual run)
#   launchd runs this at 06:00 daily     (automated)
#
# Environment:
#   HEALTH_ENV        — dev/prd (default: dev)
#   NTFY_TOPIC        — ntfy.sh topic for notifications (optional)
#   OURA_DATA_LAKE_ROOT — override data lake path (optional)

set -euo pipefail

# --- Configuration ---
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PLATFORM_ROOT="${REPO_ROOT}/health_unified_platform"
VENV_PYTHON="${REPO_ROOT}/.venv/bin/python3"
VENV_AI_PYTHON="${REPO_ROOT}/.venv-ai/bin/python3"
LOG_DIR="/Users/Shared/data_lake/logs/daily_sync"
LOG_FILE="${LOG_DIR}/$(date +%Y-%m-%d).log"
HEALTH_ENV="${HEALTH_ENV:-dev}"
NTFY_TOPIC="${NTFY_TOPIC:-}"

export HEALTH_ENV
export PYTHONPATH="${PLATFORM_ROOT}"

mkdir -p "${LOG_DIR}"

# --- Logging ---
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "${LOG_FILE}"
}

notify() {
    local title="$1"
    local message="$2"
    local priority="${3:-default}"

    if [[ -n "${NTFY_TOPIC}" ]]; then
        curl -s \
            -H "Title: ${title}" \
            -H "Priority: ${priority}" \
            -d "${message}" \
            "https://ntfy.sh/${NTFY_TOPIC}" >> "${LOG_FILE}" 2>&1 || true
    fi
}

# --- Error handler ---
on_error() {
    local exit_code=$?
    local line_no=$1
    log "ERROR: Script failed at line ${line_no} with exit code ${exit_code}"
    notify "Health Sync FAILED" "Daily sync failed at line ${line_no}. Check log: ${LOG_FILE}" "urgent"
    exit ${exit_code}
}
trap 'on_error ${LINENO}' ERR

# --- Main pipeline ---
log "=== Daily Health Sync Starting [env: ${HEALTH_ENV}] ==="

# Step 1: Oura data fetch
log "Step 1/4: Fetching Oura data..."
cd "${PLATFORM_ROOT}/health_platform/source_connectors/oura"
"${VENV_PYTHON}" run_oura.py >> "${LOG_FILE}" 2>&1
log "Step 1/4: Oura fetch complete"

# Step 2: Bronze ingestion (read parquet → DuckDB bronze tables)
log "Step 2/4: Running bronze ingestion..."
cd "${PLATFORM_ROOT}"
"${VENV_PYTHON}" -m health_platform.transformation_logic.ingestion_engine >> "${LOG_FILE}" 2>&1
log "Step 2/4: Bronze ingestion complete"

# Step 3: Silver merge (bronze → silver deduplicated tables)
log "Step 3/4: Running silver merges..."
MERGE_DIR="${PLATFORM_ROOT}/health_platform/transformation_logic/dbt/merge"
cd "${MERGE_DIR}"

MERGE_COUNT=0
MERGE_ERRORS=0

for sql_file in silver/merge_oura_*.sql; do
    if [[ -f "${sql_file}" ]]; then
        log "  Merging: ${sql_file}"
        if "${VENV_PYTHON}" run_merge.py "${sql_file}" >> "${LOG_FILE}" 2>&1; then
            MERGE_COUNT=$((MERGE_COUNT + 1))
        else
            MERGE_ERRORS=$((MERGE_ERRORS + 1))
            log "  WARNING: Merge failed for ${sql_file}"
        fi
    fi
done

log "Step 3/4: Silver merge complete (${MERGE_COUNT} succeeded, ${MERGE_ERRORS} failed)"

# Step 4: Generate daily summary + embedding for yesterday
# (yesterday because today's data may still be incomplete)
log "Step 4/4: Generating daily summary..."
cd "${PLATFORM_ROOT}"
"${VENV_AI_PYTHON}" -c "
import duckdb
import os
from datetime import date, timedelta
from health_platform.ai.text_generator import generate_summary_for_pipeline

db_path = os.environ.get('HEALTH_DB_PATH', '/Users/Shared/data_lake/database/health_dw_dev.db')
yesterday = date.today() - timedelta(days=1)

con = duckdb.connect(db_path)
try:
    summary = generate_summary_for_pipeline(con, yesterday)
    if summary:
        print(f'Summary generated for {yesterday}: {len(summary)} chars')
    else:
        print(f'No data available for {yesterday}')
finally:
    con.close()
" >> "${LOG_FILE}" 2>&1 || log "  WARNING: Summary generation failed (non-critical)"

log "Step 4/4: Daily summary complete"

# --- Generate embedding for the new summary ---
log "Bonus: Generating embedding for yesterday's summary..."
"${VENV_AI_PYTHON}" -c "
import duckdb
import os
from health_platform.ai.embedding_engine import EmbeddingEngine

db_path = os.environ.get('HEALTH_DB_PATH', '/Users/Shared/data_lake/database/health_dw_dev.db')

con = duckdb.connect(db_path)
try:
    engine = EmbeddingEngine()
    count = engine.backfill_daily_summaries(con)
    print(f'Embedded {count} summaries')
finally:
    con.close()
" >> "${LOG_FILE}" 2>&1 || log "  WARNING: Embedding generation failed (non-critical)"

# --- Done ---
log "=== Daily Health Sync Complete ==="
notify "Health Sync OK" "Daily sync completed. ${MERGE_COUNT} merges, ${MERGE_ERRORS} errors." "low"
