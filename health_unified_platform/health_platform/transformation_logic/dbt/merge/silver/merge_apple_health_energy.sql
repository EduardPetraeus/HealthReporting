-- merge_apple_health_energy.sql
-- Per-source merge: Apple Health -> silver.daily_energy_by_source
-- One row per date + source_name. Keeps sources separate to prevent double-counting.
--
-- Sources:
--   bronze.stg_apple_health_active_energy_burned
--   bronze.stg_apple_health_basal_energy_burned
--
-- Usage: python run_merge.py silver/merge_apple_health_energy.sql

-- Step 1: Aggregate each source to daily level, then full outer join
CREATE OR REPLACE TABLE silver.daily_energy_by_source__staging AS
WITH active_daily AS (
    SELECT
        startDate::DATE            AS date,
        sourceName                 AS source_name,
        SUM(value::DOUBLE)         AS active_energy_kcal,
        COUNT(*)                   AS active_sessions
    FROM bronze.stg_apple_health_active_energy_burned
    WHERE startDate IS NOT NULL
      AND value::DOUBLE > 0        -- exclude rare negative noise values
    GROUP BY 1, 2
),
basal_daily AS (
    SELECT
        startDate::DATE            AS date,
        sourceName                 AS source_name,
        SUM(value::DOUBLE)         AS basal_energy_kcal
    FROM bronze.stg_apple_health_basal_energy_burned
    WHERE startDate IS NOT NULL
      AND value::DOUBLE > 0
    GROUP BY 1, 2
),
combined AS (
    SELECT
        coalesce(a.date, b.date)               AS date,
        coalesce(a.source_name, b.source_name) AS source_name,
        a.active_energy_kcal,
        a.active_sessions,
        b.basal_energy_kcal
    FROM active_daily a
    FULL OUTER JOIN basal_daily b
        ON a.date = b.date AND a.source_name = b.source_name
)
SELECT
    -- Surrogate date key
    (year(date) * 10000 + month(date) * 100 + day(date))::INTEGER AS sk_date,

    -- Business columns
    date,
    source_name,
    ROUND(active_energy_kcal, 3)                                  AS active_energy_kcal,
    ROUND(basal_energy_kcal, 3)                                   AS basal_energy_kcal,
    ROUND(coalesce(active_energy_kcal, 0)
        + coalesce(basal_energy_kcal, 0), 3)                      AS total_energy_kcal,
    coalesce(active_sessions, 0)::INTEGER                         AS active_sessions,

    -- Business key: one row per date + source
    md5(
        coalesce(cast(date AS VARCHAR), '') || '||' ||
        coalesce(source_name, '')
    )                                                             AS business_key_hash,

    -- Row hash: change detection
    md5(
        coalesce(cast(active_energy_kcal AS VARCHAR), '') || '||' ||
        coalesce(cast(basal_energy_kcal AS VARCHAR), '') || '||' ||
        coalesce(cast(active_sessions AS VARCHAR), '')
    )                                                             AS row_hash,

    current_timestamp                                             AS load_datetime

FROM combined;

-- Step 2: Merge staging into silver.daily_energy_by_source
MERGE INTO silver.daily_energy_by_source AS target
USING silver.daily_energy_by_source__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN
  UPDATE SET
    sk_date              = src.sk_date,
    date                 = src.date,
    source_name          = src.source_name,
    active_energy_kcal   = src.active_energy_kcal,
    basal_energy_kcal    = src.basal_energy_kcal,
    total_energy_kcal    = src.total_energy_kcal,
    active_sessions      = src.active_sessions,
    row_hash             = src.row_hash,
    update_datetime      = current_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    sk_date, date, source_name,
    active_energy_kcal, basal_energy_kcal, total_energy_kcal,
    active_sessions, business_key_hash, row_hash,
    load_datetime, update_datetime
  )
  VALUES (
    src.sk_date, src.date, src.source_name,
    src.active_energy_kcal, src.basal_energy_kcal, src.total_energy_kcal,
    src.active_sessions, src.business_key_hash, src.row_hash,
    current_timestamp, current_timestamp
  );

-- Step 3: Drop staging table
DROP TABLE IF EXISTS silver.daily_energy_by_source__staging;
