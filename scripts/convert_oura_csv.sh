#!/usr/bin/env bash
# convert_oura_csv.sh
# Converts 7 Oura CSV exports (semicolon-delimited) to parquet files.
# Source: /Users/Shared/data_lake/Oura/csv/App Data/
# Target: /Users/Shared/data_lake/oura_csv/raw/{endpoint}/
#
# Usage: bash scripts/convert_oura_csv.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
CONVERTER="${REPO_ROOT}/health_unified_platform/health_platform/source_connectors/csv_to_parquet.py"
PYTHON="${REPO_ROOT}/.venv/bin/python"

SOURCE_DIR="/Users/Shared/data_lake/Oura/csv/App Data"
TARGET_BASE="/Users/Shared/data_lake/oura_csv/raw"

# Map: csv_filename -> endpoint_name
declare -A CSV_MAP
CSV_MAP["DailyResilience.csv"]="dailyresilience"
CSV_MAP["DailyCardiovascularAge.csv"]="dailycardiovascularage"
CSV_MAP["DaytimeStress.csv"]="daytimestress"
CSV_MAP["Temperature.csv"]="temperature"
CSV_MAP["SleepTime.csv"]="sleeptime"
CSV_MAP["EnhancedTag.csv"]="enhancedtag"
CSV_MAP["SleepModel.csv"]="sleepmodel"

echo "=== Oura CSV to Parquet Conversion ==="
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
        --delimiter ";"

    echo "  OK: $(ls -1 "${output_dir}"/*.parquet 2>/dev/null | wc -l | tr -d ' ') parquet file(s)"
done

echo ""
echo "=== Done ==="
