-- =============================================================================
-- dim_date.sql
-- Gold dimension: calendar date dimension for all date-grain analysis
--
-- Target: gold.dim_date (DuckDB local)
-- Materialization: CREATE OR REPLACE TABLE (static, regenerate periodically)
-- Ported from: databricks/gold/sql/dim_date.sql
--
-- DuckDB changes:
--   explode(sequence(...)) -> UNNEST(generate_series(...))
--   date_format(date, 'yyyyMMdd') -> strftime(date, '%Y%m%d')
--   date_format(date, 'MMMM') -> strftime(date, '%B')
--   date_format(date, 'EEEE') -> strftime(date, '%A')
--   dayofweek: Spark 1=Sun..7=Sat -> DuckDB 0=Sun..6=Sat
--   dayofmonth(date) -> day(date)
--   datediff(current_date(), date) -> current_date - date
-- =============================================================================

CREATE OR REPLACE TABLE gold.dim_date AS

WITH date_series AS (
    SELECT CAST(
        UNNEST(generate_series(DATE '2020-01-01', DATE '2030-12-31', INTERVAL '1' DAY))
    AS DATE) AS date
)

SELECT
    CAST(strftime(date, '%Y%m%d') AS INTEGER)              AS sk_date,
    date,
    YEAR(date)                                              AS year,
    QUARTER(date)                                           AS quarter,
    MONTH(date)                                             AS month,
    strftime(date, '%B')                                    AS month_name,
    weekofyear(date)                                        AS iso_week,
    dayofweek(date)                                         AS day_of_week,
    strftime(date, '%A')                                    AS day_name,
    day(date)                                               AS day_of_month,
    dayofyear(date)                                         AS day_of_year,
    dayofweek(date) IN (0, 6)                               AS is_weekend,
    CASE
        WHEN dayofweek(date) IN (0, 6) THEN 'weekend'
        ELSE 'weekday'
    END                                                     AS day_type,
    CASE
        WHEN MONTH(date) IN (3, 4, 5)   THEN 'spring'
        WHEN MONTH(date) IN (6, 7, 8)   THEN 'summer'
        WHEN MONTH(date) IN (9, 10, 11) THEN 'autumn'
        ELSE 'winter'
    END                                                     AS season,
    strftime(date, '%Y-%m')                                 AS year_month,
    current_date - date                                     AS days_ago

FROM date_series
ORDER BY date
