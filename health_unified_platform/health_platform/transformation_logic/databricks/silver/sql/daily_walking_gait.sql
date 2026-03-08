-- =============================================================================
-- daily_walking_gait.sql
-- Silver: Apple Health daily walking gait metrics (composite)
--
-- Source: Multiple Apple Health mobility sources aggregated daily
-- Note: This is a composite table built from walking_speed, walking_step_length,
--       walking_asymmetry, walking_double_support, walking_heart_rate_avg,
--       and walking_steadiness bronze sources.
--
-- Business key: business_key_hash (date)
-- Change detection: row_hash over all gait metrics
-- =============================================================================

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS health_dw.silver.daily_walking_gait (
    sk_date                       INTEGER   NOT NULL,
    date                          DATE      NOT NULL,
    walking_speed_avg_km_hr       DOUBLE,
    walking_step_length_avg_cm    DOUBLE,
    walking_asymmetry_avg_pct     DOUBLE,
    walking_double_support_avg_pct DOUBLE,
    walking_heart_rate_avg        DOUBLE,
    walking_steadiness_pct        DOUBLE,
    business_key_hash             STRING    NOT NULL,
    row_hash                      STRING    NOT NULL,
    load_datetime                 TIMESTAMP NOT NULL,
    update_datetime               TIMESTAMP NOT NULL
)
USING DELTA;

-- COMMAND ----------

CREATE OR REPLACE TABLE health_dw.silver.daily_walking_gait_staging AS
WITH daily_speed AS (
    SELECT to_date(startDate) AS date, AVG(CAST(value AS DOUBLE)) AS avg_speed
    FROM health_dw.bronze.stg_apple_health_walking_speed
    WHERE startDate IS NOT NULL
    GROUP BY to_date(startDate)
),
daily_step_length AS (
    SELECT to_date(startDate) AS date, AVG(CAST(value AS DOUBLE)) AS avg_step_length
    FROM health_dw.bronze.stg_apple_health_walking_step_length
    WHERE startDate IS NOT NULL
    GROUP BY to_date(startDate)
),
daily_asymmetry AS (
    SELECT to_date(startDate) AS date, AVG(CAST(value AS DOUBLE)) AS avg_asymmetry
    FROM health_dw.bronze.stg_apple_health_walking_asymmetry
    WHERE startDate IS NOT NULL
    GROUP BY to_date(startDate)
),
daily_double_support AS (
    SELECT to_date(startDate) AS date, AVG(CAST(value AS DOUBLE)) AS avg_double_support
    FROM health_dw.bronze.stg_apple_health_walking_double_support
    WHERE startDate IS NOT NULL
    GROUP BY to_date(startDate)
),
daily_walk_hr AS (
    SELECT to_date(startDate) AS date, AVG(CAST(value AS DOUBLE)) AS avg_walk_hr
    FROM health_dw.bronze.stg_apple_health_walking_heart_rate_avg
    WHERE startDate IS NOT NULL
    GROUP BY to_date(startDate)
),
daily_steadiness AS (
    SELECT to_date(startDate) AS date, AVG(CAST(value AS DOUBLE)) AS avg_steadiness
    FROM health_dw.bronze.stg_apple_health_walking_steadiness
    WHERE startDate IS NOT NULL
    GROUP BY to_date(startDate)
),
date_spine AS (
    SELECT DISTINCT date FROM daily_speed
    UNION SELECT DISTINCT date FROM daily_step_length
    UNION SELECT DISTINCT date FROM daily_asymmetry
    UNION SELECT DISTINCT date FROM daily_double_support
    UNION SELECT DISTINCT date FROM daily_walk_hr
    UNION SELECT DISTINCT date FROM daily_steadiness
)
SELECT
    year(ds.date) * 10000 + month(ds.date) * 100 + dayofmonth(ds.date) AS sk_date,
    ds.date,
    ROUND(sp.avg_speed, 2)                                AS walking_speed_avg_km_hr,
    ROUND(sl.avg_step_length, 2)                          AS walking_step_length_avg_cm,
    ROUND(a.avg_asymmetry, 2)                             AS walking_asymmetry_avg_pct,
    ROUND(dds.avg_double_support, 2)                      AS walking_double_support_avg_pct,
    ROUND(whr.avg_walk_hr, 1)                             AS walking_heart_rate_avg,
    ROUND(st.avg_steadiness, 2)                           AS walking_steadiness_pct,
    sha2(cast(ds.date AS STRING), 256)                    AS business_key_hash,
    sha2(
        concat_ws('||',
            coalesce(cast(sp.avg_speed AS STRING), ''),
            coalesce(cast(sl.avg_step_length AS STRING), ''),
            coalesce(cast(a.avg_asymmetry AS STRING), ''),
            coalesce(cast(dds.avg_double_support AS STRING), ''),
            coalesce(cast(whr.avg_walk_hr AS STRING), ''),
            coalesce(cast(st.avg_steadiness AS STRING), '')
        ), 256
    )                                                     AS row_hash,
    current_timestamp()                                   AS load_datetime
FROM date_spine ds
LEFT JOIN daily_speed sp            ON ds.date = sp.date
LEFT JOIN daily_step_length sl      ON ds.date = sl.date
LEFT JOIN daily_asymmetry a         ON ds.date = a.date
LEFT JOIN daily_double_support dds  ON ds.date = dds.date
LEFT JOIN daily_walk_hr whr         ON ds.date = whr.date
LEFT JOIN daily_steadiness st       ON ds.date = st.date;

-- COMMAND ----------

MERGE INTO health_dw.silver.daily_walking_gait AS target
USING health_dw.silver.daily_walking_gait_staging AS source
ON target.business_key_hash = source.business_key_hash

WHEN MATCHED AND target.row_hash <> source.row_hash THEN
    UPDATE SET
        target.sk_date                        = source.sk_date,
        target.date                           = source.date,
        target.walking_speed_avg_km_hr        = source.walking_speed_avg_km_hr,
        target.walking_step_length_avg_cm     = source.walking_step_length_avg_cm,
        target.walking_asymmetry_avg_pct      = source.walking_asymmetry_avg_pct,
        target.walking_double_support_avg_pct = source.walking_double_support_avg_pct,
        target.walking_heart_rate_avg         = source.walking_heart_rate_avg,
        target.walking_steadiness_pct         = source.walking_steadiness_pct,
        target.row_hash                       = source.row_hash,
        target.update_datetime                = current_timestamp()

WHEN NOT MATCHED THEN
    INSERT (
        sk_date, date, walking_speed_avg_km_hr, walking_step_length_avg_cm,
        walking_asymmetry_avg_pct, walking_double_support_avg_pct,
        walking_heart_rate_avg, walking_steadiness_pct,
        business_key_hash, row_hash, load_datetime, update_datetime
    )
    VALUES (
        source.sk_date, source.date, source.walking_speed_avg_km_hr,
        source.walking_step_length_avg_cm, source.walking_asymmetry_avg_pct,
        source.walking_double_support_avg_pct, source.walking_heart_rate_avg,
        source.walking_steadiness_pct,
        source.business_key_hash, source.row_hash,
        current_timestamp(), current_timestamp()
    );

-- COMMAND ----------

DROP TABLE IF EXISTS health_dw.silver.daily_walking_gait_staging;
