---
name: data-quality-checker
description: Run data quality checks on bronze or silver tables. Use when asked to check data quality, validate data, audit freshness, or flag anomalies.
---

# Data Quality Checker

Run DQ checks on the health data platform.

## Freshness checks
For each silver table, verify:
```sql
SELECT
  '<table>'                          AS table_name,
  MAX(_ingested_at)                  AS last_ingested,
  DATEDIFF('hour', MAX(_ingested_at), current_timestamp()) AS hours_stale
FROM silver.<table>
```
Flag tables where `hours_stale > 25` (missed daily ingestion).

## Row count checks
- Bronze vs silver row count ratio — flag if silver has < 90% of bronze rows
- Flag if any table has 0 rows

## Null rate checks
For key columns (date, source_system, primary metric), report:
```sql
SELECT
  COUNT(*) FILTER (WHERE <col> IS NULL) * 100.0 / COUNT(*) AS null_pct
FROM silver.<table>
```
Flag if null_pct > 5% on any key column.

## Anomaly detection (simple)
- Flag rows where a metric is > 3 standard deviations from the mean
- Report as: `[ANOMALY] table: silver.<x>, column: <y>, value: <z>, z-score: <n>`

## Output format
```
FRESHNESS:  ✓ oura_sleep (2h ago) | ✗ apple_health_steps (STALE: 30h)
ROW COUNTS: ✓ oura_sleep 1,204 rows | ✗ apple_health_steps 0 rows
NULLS:      ✓ all key columns < 5% null
ANOMALIES:  [ANOMALY] oura_sleep.hrv_rmssd = 142 (z=3.4)
```

## Scale-up note
Enterprise upgrade: replace with DLT `@dlt.expect` expectations for row-level quarantine and Databricks DQ monitoring dashboard.
