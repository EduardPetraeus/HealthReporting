-- merge_apple_health_walking_gait.sql
-- Per-source merge: Apple Health walking metrics -> silver.daily_walking_gait
-- Aggregates 6 bronze tables to daily level and merges into one wide model.
--
-- Sources:
--   bronze.stg_apple_health_walking_speed
--   bronze.stg_apple_health_walking_step_length
--   bronze.stg_apple_health_walking_asymmetry
--   bronze.stg_apple_health_walking_double_support
--   bronze.stg_apple_health_walking_heart_rate_avg
--   bronze.stg_apple_health_walking_steadiness
--
-- Usage: python run_merge.py silver/merge_apple_health_walking_gait.sql

-- Step 1: Aggregate each source to daily level and join into staging
CREATE OR REPLACE TABLE silver.daily_walking_gait__staging AS
WITH speed_daily AS (
    SELECT
        startDate::DATE                AS date,
        AVG(value::DOUBLE)             AS walking_speed_avg_km_hr
    FROM bronze.stg_apple_health_walking_speed
    WHERE startDate IS NOT NULL
    GROUP BY 1
),
step_length_daily AS (
    SELECT
        startDate::DATE                AS date,
        AVG(value::DOUBLE)             AS walking_step_length_avg_cm
    FROM bronze.stg_apple_health_walking_step_length
    WHERE startDate IS NOT NULL
    GROUP BY 1
),
asymmetry_daily AS (
    SELECT
        startDate::DATE                AS date,
        AVG(value::DOUBLE)             AS walking_asymmetry_avg_pct
    FROM bronze.stg_apple_health_walking_asymmetry
    WHERE startDate IS NOT NULL
    GROUP BY 1
),
double_support_daily AS (
    SELECT
        startDate::DATE                AS date,
        AVG(value::DOUBLE)             AS walking_double_support_avg_pct
    FROM bronze.stg_apple_health_walking_double_support
    WHERE startDate IS NOT NULL
    GROUP BY 1
),
hr_avg_daily AS (
    SELECT
        startDate::DATE                AS date,
        AVG(value::DOUBLE)             AS walking_heart_rate_avg
    FROM bronze.stg_apple_health_walking_heart_rate_avg
    WHERE startDate IS NOT NULL
    GROUP BY 1
),
steadiness_daily AS (
    -- Already 1 row per day — take AVG to handle any edge-case duplicates
    SELECT
        startDate::DATE                AS date,
        AVG(value::DOUBLE)             AS walking_steadiness_pct
    FROM bronze.stg_apple_health_walking_steadiness
    WHERE startDate IS NOT NULL
    GROUP BY 1
),
all_dates AS (
    SELECT date FROM speed_daily
    UNION SELECT date FROM step_length_daily
    UNION SELECT date FROM asymmetry_daily
    UNION SELECT date FROM double_support_daily
    UNION SELECT date FROM hr_avg_daily
    UNION SELECT date FROM steadiness_daily
)
SELECT
    -- Surrogate date key
    (year(d.date) * 10000 + month(d.date) * 100 + day(d.date))::INTEGER AS sk_date,

    -- Business columns
    d.date,
    sp.walking_speed_avg_km_hr,
    sl.walking_step_length_avg_cm,
    asy.walking_asymmetry_avg_pct,
    ds.walking_double_support_avg_pct,
    hr.walking_heart_rate_avg,
    st.walking_steadiness_pct,

    -- Business key: one row per date
    md5(cast(d.date AS VARCHAR))       AS business_key_hash,

    -- Row hash: change detection across all metrics
    md5(
        coalesce(cast(sp.walking_speed_avg_km_hr AS VARCHAR), '')        || '||' ||
        coalesce(cast(sl.walking_step_length_avg_cm AS VARCHAR), '')     || '||' ||
        coalesce(cast(asy.walking_asymmetry_avg_pct AS VARCHAR), '')     || '||' ||
        coalesce(cast(ds.walking_double_support_avg_pct AS VARCHAR), '') || '||' ||
        coalesce(cast(hr.walking_heart_rate_avg AS VARCHAR), '')         || '||' ||
        coalesce(cast(st.walking_steadiness_pct AS VARCHAR), '')
    )                                  AS row_hash,

    current_timestamp                  AS load_datetime

FROM all_dates d
LEFT JOIN speed_daily        sp  ON d.date = sp.date
LEFT JOIN step_length_daily  sl  ON d.date = sl.date
LEFT JOIN asymmetry_daily    asy ON d.date = asy.date
LEFT JOIN double_support_daily ds ON d.date = ds.date
LEFT JOIN hr_avg_daily       hr  ON d.date = hr.date
LEFT JOIN steadiness_daily   st  ON d.date = st.date;

-- Step 2: Merge staging into silver.daily_walking_gait
MERGE INTO silver.daily_walking_gait AS target
USING silver.daily_walking_gait__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN
  UPDATE SET
    sk_date                        = src.sk_date,
    date                           = src.date,
    walking_speed_avg_km_hr        = src.walking_speed_avg_km_hr,
    walking_step_length_avg_cm     = src.walking_step_length_avg_cm,
    walking_asymmetry_avg_pct      = src.walking_asymmetry_avg_pct,
    walking_double_support_avg_pct = src.walking_double_support_avg_pct,
    walking_heart_rate_avg         = src.walking_heart_rate_avg,
    walking_steadiness_pct         = src.walking_steadiness_pct,
    row_hash                       = src.row_hash,
    update_datetime                = current_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    sk_date,
    date,
    walking_speed_avg_km_hr,
    walking_step_length_avg_cm,
    walking_asymmetry_avg_pct,
    walking_double_support_avg_pct,
    walking_heart_rate_avg,
    walking_steadiness_pct,
    business_key_hash,
    row_hash,
    load_datetime,
    update_datetime
  )
  VALUES (
    src.sk_date,
    src.date,
    src.walking_speed_avg_km_hr,
    src.walking_step_length_avg_cm,
    src.walking_asymmetry_avg_pct,
    src.walking_double_support_avg_pct,
    src.walking_heart_rate_avg,
    src.walking_steadiness_pct,
    src.business_key_hash,
    src.row_hash,
    current_timestamp,
    current_timestamp
  );

-- Step 3: Drop staging table
DROP TABLE IF EXISTS silver.daily_walking_gait__staging;
