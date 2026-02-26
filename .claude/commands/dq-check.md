Run the data-quality-checker agent on all silver tables in the local DuckDB.

Use the data-quality-checker agent to:
1. Connect to the DuckDB file for the current HEALTH_ENV (default: dev)
2. Run freshness, row count, null rate, and anomaly checks on all silver tables
3. Output results in the standard format:
   FRESHNESS:  ✓/✗ per table
   ROW COUNTS: ✓/✗ per table
   NULLS:      ✓/✗ key columns
   ANOMALIES:  [ANOMALY] entries if any
4. Flag any table where hours_stale > 25 or row count = 0
