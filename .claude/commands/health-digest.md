Generate a quick health data digest from the local DuckDB.

1. Connect to `health_dw_dev.db` using DuckDB
2. For each available silver table, summarize:
   - Row count
   - Date range (min/max date)
   - Last ingested timestamp
3. Flag any table where `max(_ingested_at)` is older than 48 hours
4. Output a clean markdown summary
