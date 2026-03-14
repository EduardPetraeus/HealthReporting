#!/usr/bin/env bash
# convert_withings_csv.sh
# Converts Withings CSV exports (comma-delimited) to parquet files.
# CSV data goes to _csv/ subdirectories to avoid column conflicts with API parquet.
# Source: /Users/Shared/data_lake/withings/csv/
# Target: /Users/Shared/data_lake/withings/raw/{endpoint}_csv/
#
# Usage: bash scripts/convert_withings_csv.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
CONVERTER="${REPO_ROOT}/health_unified_platform/health_platform/source_connectors/csv_to_parquet.py"
PYTHON="${REPO_ROOT}/.venv/bin/python"

SOURCE_DIR="/Users/Shared/data_lake/withings/csv"
TARGET_BASE="/Users/Shared/data_lake/withings/raw"

# Map: csv_filename -> endpoint_name (writes to {endpoint}_csv/ directories)
declare -A CSV_MAP
CSV_MAP["weight.csv"]="weight_csv"
CSV_MAP["bp.csv"]="blood_pressure_csv"
CSV_MAP["sleep.csv"]="sleep_csv"
CSV_MAP["body_temperature.csv"]="body_temperature_csv"
CSV_MAP["signal.csv"]="signal"
CSV_MAP["pwv.csv"]="pwv"
CSV_MAP["activities.csv"]="activities_csv"

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
