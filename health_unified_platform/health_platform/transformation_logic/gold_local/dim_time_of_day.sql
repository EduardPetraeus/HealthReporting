-- =============================================================================
-- dim_time_of_day.sql
-- Gold dimension: time-of-day dimension for intraday analysis
--
-- Target: gold.dim_time_of_day (DuckDB local)
-- Materialization: CREATE OR REPLACE TABLE (static, 1440 rows)
-- Ported from: databricks/gold/sql/dim_time_of_day.sql
--
-- DuckDB changes:
--   explode(sequence(0, 23)) -> UNNEST(generate_series(0, 23))
-- =============================================================================

CREATE OR REPLACE TABLE gold.dim_time_of_day AS

WITH hours AS (
    SELECT UNNEST(generate_series(0, 23)) AS hour
),
minutes AS (
    SELECT UNNEST(generate_series(0, 59)) AS minute
),
time_series AS (
    SELECT h.hour, m.minute
    FROM hours h
    CROSS JOIN minutes m
)

SELECT
    LPAD(CAST(hour AS VARCHAR), 2, '0') || LPAD(CAST(minute AS VARCHAR), 2, '0')
                                                        AS sk_time,
    hour,
    minute,
    LPAD(CAST(hour AS VARCHAR), 2, '0') || ':' || LPAD(CAST(minute AS VARCHAR), 2, '0')
                                                        AS time_label,
    CASE
        WHEN hour BETWEEN 0  AND 5  THEN 'night'
        WHEN hour BETWEEN 6  AND 8  THEN 'early_morning'
        WHEN hour BETWEEN 9  AND 11 THEN 'morning'
        WHEN hour BETWEEN 12 AND 16 THEN 'afternoon'
        WHEN hour BETWEEN 17 AND 20 THEN 'evening'
        ELSE 'late_evening'
    END                                                 AS time_bucket,
    hour < 7 OR hour >= 22                              AS is_sleeping_hours,
    hour >= 7 AND hour < 22                             AS is_active_hours

FROM time_series
ORDER BY hour, minute
