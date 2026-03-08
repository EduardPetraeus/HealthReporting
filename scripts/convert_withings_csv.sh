#!/usr/bin/env bash
# convert_withings_csv.sh
# Converts 6 Withings CSV exports (comma-delimited) to parquet files.
# Source: /Users/Shared/data_lake/withings/csv/
# Target: /Users/Shared/data_lake/withings/raw/{endpoint}/
#
# Usage: bash scripts/convert_withings_csv.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
CONVERTER="${REPO_ROOT}/health_unified_platform/health_platform/source_connectors/csv_to_parquet.py"
PYTHON="${REPO_ROOT}/.venv/bin/python"

SOURCE_DIR="/Users/Shared/data_lake/withings/csv"
TARGET_BASE="/Users/Shared/data_lake/withings/raw"

# Map: csv_filename -> endpoint_name
declare -A CSV_MAP
CSV_MAP["raw_tracker_sleep.csv"]="sleep"
CSV_MAP["raw_tracker_blood-pressure.csv"]="blood_pressure"
CSV_MAP["raw_tracker_weight.csv"]="weight"
CSV_MAP["raw_tracker_body-temperature.csv"]="body_temperature"
CSV_MAP["raw_tracker_signal.csv"]="signal"
CSV_MAP["raw_tracker_pwv.csv"]="pwv"

echo "=== Withings CSV to Parquet Conversion ==="
echo "Source: ${SOURCE_DIR}"
echo ""

for csv_file in "${!CSV_MAP[@]}"; do
    endpoint="${CSV_MAP[$csv_file]}"
    input_path="${SOURCE_DIR}/${csv_file}"
    output_dir="${TARGET_BASE}/${endpoint}"

    if [ ! -f "${input_path}" ]; then
        echo "SKIP: ${csv_file} (file not found)"
        continue
    fi

    echo "Converting: ${csv_file} -> ${endpoint}/"
    mkdir -p "${output_dir}"
    "${PYTHON}" "${CONVERTER}" \
        --input "${input_path}" \
        --output "${output_dir}" \
        --delimiter ","

    echo "  OK: $(ls -1 "${output_dir}"/*.parquet 2>/dev/null | wc -l | tr -d ' ') parquet file(s)"
done

echo ""
echo "=== Done ==="
