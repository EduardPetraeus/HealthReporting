# Databricks Gold SQL

One SQL file per gold view or table. Files use `-- COMMAND ----------` as Databricks SQL notebook cell separator.

## Implemented

- `daily_heart_rate_summary.sql` — daily average heart rate aggregation (YAML-driven)
- `vw_daily_annotations.sql` — valid daily annotations filtered from `silver.daily_annotations`
- `vw_heart_rate_avg_per_day.sql` — daily average BPM from `silver.heart_rate`

## Pattern

Gold files create views or aggregated tables from silver sources:

```sql
-- COMMAND ----------
CREATE OR REPLACE VIEW health_dw.gold.vw_<name> AS
SELECT ...
FROM health_dw.silver.<source>
WHERE ...;
```
