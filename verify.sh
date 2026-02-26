#!/bin/bash
# verify.sh — Local platform verification script
# Usage: ./verify.sh [dev|prd]
# Boris Cherny's #1 tip: give Claude a feedback loop to verify its work

set -e

ENV=${1:-dev}
DB="health_dw_${ENV}.db"

echo "=================================================="
echo "  Health Platform — Verification (${ENV})"
echo "=================================================="

# 1. Check venv
if [ ! -f ".venv/bin/python" ]; then
  echo "ERROR: .venv missing. Run: python3 -m venv .venv && pip install -r requirements.txt"
  exit 1
fi

# 2. Check DuckDB file
if [ ! -f "${DB}" ]; then
  echo "WARN: ${DB} not found — skipping table checks (run ingestion first)"
else
  echo ""
  echo "--- Silver table status ---"
  .venv/bin/python3 - <<PYEOF
import duckdb, sys
db = duckdb.connect("${DB}", read_only=True)
tables = db.execute("SHOW TABLES").fetchall()
silver = [t[0] for t in tables if not t[0].startswith("stg_")]
if not silver:
    print("  No silver tables found yet")
else:
    for t in sorted(silver):
        try:
            row = db.execute(f"SELECT COUNT(*) as n, MIN(_ingested_at) as first, MAX(_ingested_at) as last FROM {t}").fetchone()
            print(f"  {t:<40} rows={row[0]:<8} last={str(row[2])[:19] if row[2] else 'N/A'}")
        except Exception as e:
            print(f"  {t:<40} ERROR: {e}")
db.close()
PYEOF
fi

# 3. Validate YAML configs
echo ""
echo "--- YAML config validation ---"
CONFIG_DIR="health_unified_platform/health_environment/config/databricks/sources"
if [ -d "${CONFIG_DIR}" ]; then
  count=$(find "${CONFIG_DIR}" -name "*.yml" -o -name "*.yaml" | wc -l | tr -d ' ')
  echo "  Source configs found: ${count}"
  .venv/bin/python3 - <<PYEOF
import yaml, os, glob
config_dir = "${CONFIG_DIR}"
files = glob.glob(f"{config_dir}/**/*.yml", recursive=True) + glob.glob(f"{config_dir}/**/*.yaml", recursive=True)
errors = []
for f in files:
    try:
        yaml.safe_load(open(f))
    except Exception as e:
        errors.append(f"  INVALID: {f} — {e}")
if errors:
    for e in errors: print(e)
else:
    print(f"  All {len(files)} YAML files valid")
PYEOF
else
  echo "  WARN: config dir not found at ${CONFIG_DIR}"
fi

# 4. Check silver SQL count
echo ""
echo "--- Silver SQL transforms ---"
SILVER_SQL="health_unified_platform/transformation_logic/databricks/silver/sql"
if [ -d "${SILVER_SQL}" ]; then
  sql_count=$(find "${SILVER_SQL}" -name "*.sql" | wc -l | tr -d ' ')
  echo "  SQL files found: ${sql_count}"
else
  echo "  WARN: silver SQL dir not found"
fi

# 5. Run productivity tracker
echo ""
echo "--- Productivity ---"
.venv/bin/python update_productivity.py 2>/dev/null | grep -E "Commits|Velocity|Files" || true

echo ""
echo "=================================================="
echo "  Verification complete"
echo "=================================================="
