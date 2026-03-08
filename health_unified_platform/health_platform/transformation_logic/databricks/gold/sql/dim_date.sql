-- =============================================================================
-- dim_date.sql
-- Gold dimension: calendar date dimension for all date-grain analysis
--
-- Target: health_dw.gold.dim_date (Databricks)
-- Materialization: CREATE OR REPLACE TABLE (static, regenerate periodically)
-- =============================================================================

CREATE OR REPLACE TABLE {target} AS

WITH date_series AS (
    SELECT explode(sequence(DATE '2020-01-01', DATE '2030-12-31', INTERVAL 1 DAY)) AS date
)

SELECT
    CAST(date_format(date, 'yyyyMMdd') AS INTEGER)     AS sk_date,
    date,
    YEAR(date)                                          AS year,
    QUARTER(date)                                       AS quarter,
    MONTH(date)                                         AS month,
    date_format(date, 'MMMM')                           AS month_name,
    weekofyear(date)                                    AS iso_week,
    dayofweek(date)                                     AS day_of_week,
    date_format(date, 'EEEE')                           AS day_name,
    dayofmonth(date)                                    AS day_of_month,
    dayofyear(date)                                     AS day_of_year,
    dayofweek(date) IN (1, 7)                           AS is_weekend,
    CASE
        WHEN dayofweek(date) IN (1, 7) THEN 'weekend'
        ELSE 'weekday'
    END                                                 AS day_type,
    CASE
        WHEN MONTH(date) IN (3, 4, 5)   THEN 'spring'
        WHEN MONTH(date) IN (6, 7, 8)   THEN 'summer'
        WHEN MONTH(date) IN (9, 10, 11) THEN 'autumn'
        ELSE 'winter'
    END                                                 AS season,
    date_format(date, 'yyyy-MM')                        AS year_month,
    datediff(current_date(), date)                      AS days_ago

FROM date_series
ORDER BY date
