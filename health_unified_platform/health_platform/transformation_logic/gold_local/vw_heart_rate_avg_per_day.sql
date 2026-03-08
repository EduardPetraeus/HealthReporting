-- =============================================================================
-- vw_heart_rate_avg_per_day.sql
-- Gold view: average heart rate per day
--
-- Target: gold.vw_heart_rate_avg_per_day (DuckDB local)
-- Materialization: CREATE OR REPLACE VIEW
-- Ported from: databricks/gold/sql/vw_heart_rate_avg_per_day.sql
-- =============================================================================

CREATE OR REPLACE VIEW gold.vw_heart_rate_avg_per_day AS

SELECT
    sk_date,
    AVG(bpm) AS avg_bpm
FROM silver.heart_rate
GROUP BY sk_date
