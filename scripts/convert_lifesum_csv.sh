#!/bin/bash
# convert_lifesum_csv.sh — Convert Lifesum CSV exports to Parquet
#
# Converts exercise, weighins, and bodyfat CSV files to parquet format.
# Idempotent — safe to run repeatedly. Overwrites existing parquet files.
#
# Usage:
#   ./scripts/convert_lifesum_csv.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd -P)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd -P)"
PLATFORM_ROOT="${REPO_ROOT}/health_unified_platform"
VENV_PYTHON="${REPO_ROOT}/.venv/bin/python3"

CSV_DIR="/Users/Shared/data_lake/lifesum/csv"
PARQUET_DIR="/Users/Shared/data_lake/lifesum/parquet"

export PYTHONPATH="${PLATFORM_ROOT}"

SOURCES=("exercise" "weighins" "bodyfat")
CONVERTED=0
SKIPPED=0

for source in "${SOURCES[@]}"; do
    csv_file="${CSV_DIR}/${source}.csv"
    if [[ -f "${csv_file}" ]]; then
        echo "[$(date '+%H:%M:%S')] Converting ${source}.csv -> parquet..."
        "${VENV_PYTHON}" -m health_platform.source_connectors.csv_to_parquet \
            --input "${csv_file}" \
            --output "${PARQUET_DIR}" \
            --source-name "${source}"
        CONVERTED=$((CONVERTED + 1))
    else
        echo "[$(date '+%H:%M:%S')] Skipping ${source} — CSV not found: ${csv_file}"
        SKIPPED=$((SKIPPED + 1))
    fi
done

echo "[$(date '+%H:%M:%S')] Done: ${CONVERTED} converted, ${SKIPPED} skipped"
