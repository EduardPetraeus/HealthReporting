#!/bin/bash
# daily_sync.sh — Automated daily health data refresh
#
# Pipeline:
#   Step 1: Fetch data from all API sources (Oura, Withings, Strava, Weather)
#   Step 2: Bronze ingestion (parquet → DuckDB bronze tables)
#   Step 3: Silver merge (ALL sources — bronze → silver deduplicated tables)
#   Step 4: Data quality checks (YAML-driven, warnings only)
#   Step 5: Daily summary + embedding generation
#   Step 6: Correlation computation (metric relationships)
#   Step 7: Patient profile refresh (baselines + demographics)
#   Step 8: Anomaly detection + notifications (ntfy.sh)
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
LOG_DIR="${HEALTH_LOG_DIR:-/Users/Shared/data_lake/logs/daily_sync}"
LOG_FILE="${LOG_DIR}/$(date +%Y-%m-%d).log"
HEALTH_ENV="${HEALTH_ENV:-dev}"
NTFY_TOPIC="${NTFY_TOPIC:-}"

export HEALTH_ENV
export PYTHONPATH="${PLATFORM_ROOT}"

# Validate PLATFORM_ROOT resolves to the actual platform package
if [[ ! -f "${PLATFORM_ROOT}/health_platform/__init__.py" ]]; then
    echo "ERROR: PLATFORM_ROOT does not point to a valid health_platform package: ${PLATFORM_ROOT}" >&2
    exit 1
fi

# Resolve data lake root and DB path via paths.py (cross-platform)
HEALTH_DB_PATH=$("${VENV_PYTHON}" -c "from health_platform.utils.paths import get_db_path; print(get_db_path())")
export HEALTH_DB_PATH

# Validate NTFY_TOPIC (only allow safe chars for URL)
if [[ -n "${NTFY_TOPIC}" && ! "${NTFY_TOPIC}" =~ ^[a-zA-Z0-9_-]+$ ]]; then
    echo "WARNING: NTFY_TOPIC contains invalid characters — notifications disabled" >&2
    NTFY_TOPIC=""
fi

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

# --- Counters ---
FETCH_OK=0
FETCH_WARN=0
MERGE_COUNT=0
MERGE_ERRORS=0

# --- Main pipeline ---
log "=== Daily Health Sync Starting [env: ${HEALTH_ENV}] ==="

# =========================================================================
# Step 1/8: Fetch data from all API sources
# =========================================================================
log "Step 1/8: Fetching data from API sources..."

# 1a: Oura (required — primary wearable)
log "  1a: Oura..."
cd "${PLATFORM_ROOT}/health_platform/source_connectors/oura"
if "${VENV_PYTHON}" run_oura.py >> "${LOG_FILE}" 2>&1; then
    FETCH_OK=$((FETCH_OK + 1))
    log "  1a: Oura fetch complete"
else
    FETCH_WARN=$((FETCH_WARN + 1))
    log "  1a: WARNING — Oura fetch failed (continuing)"
fi

# 1b: Withings (weight + blood pressure)
log "  1b: Withings..."
cd "${PLATFORM_ROOT}/health_platform/source_connectors/withings"
if "${VENV_PYTHON}" run_withings.py >> "${LOG_FILE}" 2>&1; then
    FETCH_OK=$((FETCH_OK + 1))
    log "  1b: Withings fetch complete"
else
    FETCH_WARN=$((FETCH_WARN + 1))
    log "  1b: WARNING — Withings fetch failed (continuing)"
fi

# 1c: Strava (activities)
log "  1c: Strava..."
cd "${PLATFORM_ROOT}/health_platform/source_connectors/strava"
if "${VENV_PYTHON}" run_strava.py >> "${LOG_FILE}" 2>&1; then
    FETCH_OK=$((FETCH_OK + 1))
    log "  1c: Strava fetch complete"
else
    FETCH_WARN=$((FETCH_WARN + 1))
    log "  1c: WARNING — Strava fetch failed (continuing)"
fi

# 1d: Weather (Open-Meteo — no auth required)
log "  1d: Weather..."
cd "${PLATFORM_ROOT}/health_platform/source_connectors/weather"
if "${VENV_PYTHON}" run_weather.py >> "${LOG_FILE}" 2>&1; then
    FETCH_OK=$((FETCH_OK + 1))
    log "  1d: Weather fetch complete"
else
    FETCH_WARN=$((FETCH_WARN + 1))
    log "  1d: WARNING — Weather fetch failed (continuing)"
fi

log "Step 1/8: Fetch complete (${FETCH_OK} ok, ${FETCH_WARN} warnings)"

# =========================================================================
# Step 2/8: Bronze ingestion (read parquet → DuckDB bronze tables)
# =========================================================================
log "Step 2/8: Running bronze ingestion..."
cd "${PLATFORM_ROOT}"
"${VENV_PYTHON}" -m health_platform.transformation_logic.ingestion_engine >> "${LOG_FILE}" 2>&1
log "Step 2/8: Bronze ingestion complete"

# =========================================================================
# Step 3/8: Silver merge (ALL sources — bronze → silver deduplicated)
# =========================================================================
log "Step 3/8: Running silver merges..."
MERGE_DIR="${PLATFORM_ROOT}/health_platform/transformation_logic/dbt/merge"
cd "${MERGE_DIR}"

for sql_file in silver/merge_*.sql; do
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

log "Step 3/8: Silver merge complete (${MERGE_COUNT} succeeded, ${MERGE_ERRORS} failed)"

# =========================================================================
# Step 4/8: Data quality checks (warnings only — never stops pipeline)
# =========================================================================
log "Step 4/8: Running data quality checks..."
DQ_EXIT=0
DQ_OUTPUT=$("${VENV_PYTHON}" "${REPO_ROOT}/scripts/run_quality_checks.py" 2>&1) || DQ_EXIT=$?
log "${DQ_OUTPUT}"
if [[ ${DQ_EXIT} -ne 0 ]]; then
    log "  WARNING: Data quality issues detected"
    notify "Health DQ Warning" "${DQ_OUTPUT}" "high"
fi
log "Step 4/8: Data quality checks complete"

# =========================================================================
# Step 5/8: Generate daily summary + embedding for yesterday
# =========================================================================
# (yesterday because today's data may still be incomplete)
log "Step 5/8: Generating daily summary..."
cd "${PLATFORM_ROOT}"
"${VENV_AI_PYTHON}" -c "
import duckdb
import os
from datetime import date, timedelta
from health_platform.ai.text_generator import generate_summary_for_pipeline

db_path = os.environ['HEALTH_DB_PATH']
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

log "Step 5/8: Daily summary complete"

# --- Generate embedding for the new summary ---
log "  Generating embedding for yesterday's summary..."
"${VENV_AI_PYTHON}" -c "
import duckdb
import os
from health_platform.ai.embedding_engine import EmbeddingEngine

db_path = os.environ['HEALTH_DB_PATH']

con = duckdb.connect(db_path)
try:
    engine = EmbeddingEngine()
    count = engine.backfill_daily_summaries(con)
    print(f'Embedded {count} summaries')
finally:
    con.close()
" >> "${LOG_FILE}" 2>&1 || log "  WARNING: Embedding generation failed (non-critical)"

# =========================================================================
# Step 6/8: Correlation computation
# =========================================================================
log "Step 6/8: Computing metric correlations..."
cd "${PLATFORM_ROOT}"
"${VENV_AI_PYTHON}" -c "
import duckdb
import os
from health_platform.ai.correlation_engine import compute_all_correlations

db_path = os.environ['HEALTH_DB_PATH']

con = duckdb.connect(db_path)
try:
    count = compute_all_correlations(con)
    print(f'Computed {count} correlations')
finally:
    con.close()
" >> "${LOG_FILE}" 2>&1 || log "  WARNING: Correlation computation failed (non-critical)"

log "Step 6/8: Correlations complete"

# =========================================================================
# Step 7/8: Patient profile refresh (baselines + demographics)
# =========================================================================
log "Step 7/8: Refreshing patient profile..."
cd "${PLATFORM_ROOT}"
"${VENV_AI_PYTHON}" -c "
import duckdb
import os
from health_platform.ai.baseline_computer import compute_all_baselines, compute_demographics

db_path = os.environ['HEALTH_DB_PATH']

con = duckdb.connect(db_path)
try:
    baselines = compute_all_baselines(con)
    demographics = compute_demographics(con)
    print(f'Profile refresh: {baselines} baselines, {demographics} demographics')
finally:
    con.close()
" >> "${LOG_FILE}" 2>&1 || log "  WARNING: Profile refresh failed (non-critical)"

log "Step 7/8: Patient profile refresh complete"

# =========================================================================
# Step 8/8: Anomaly detection + notifications
# =========================================================================
log "Step 8/8: Running anomaly detection..."
cd "${PLATFORM_ROOT}"
"${VENV_AI_PYTHON}" -c "
import duckdb
import os
from health_platform.ai.anomaly_detector import AnomalyDetector, format_anomaly_report
from health_platform.ai.notification_manager import NotificationManager

db_path = os.environ['HEALTH_DB_PATH']
ntfy_topic = os.environ.get('NTFY_TOPIC', '')

con = duckdb.connect(db_path, read_only=True)
try:
    detector = AnomalyDetector(con)
    report = detector.detect(lookback_days=1)
    print(format_anomaly_report(report))

    if report.has_critical:
        manager = NotificationManager(topic=ntfy_topic if ntfy_topic else None)
        sent = manager.notify_anomaly_report(report)
        print(f'Sent {sent} notifications')
    else:
        print('No critical anomalies -- no notifications sent')
finally:
    con.close()
" >> "${LOG_FILE}" 2>&1 || log "  WARNING: Anomaly detection failed (non-critical)"

log "Step 8/8: Anomaly detection complete"

# =========================================================================
# Done
# =========================================================================
log "=== Daily Health Sync Complete ==="
log "Summary: ${FETCH_OK} sources fetched, ${FETCH_WARN} fetch warnings, ${MERGE_COUNT} merges ok, ${MERGE_ERRORS} merge errors"
notify "Health Sync OK" "Daily sync: ${FETCH_OK} fetched, ${MERGE_COUNT} merges, ${MERGE_ERRORS} errors." "low"
