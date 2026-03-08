-- =============================================================================
-- daily_energy_by_source.sql
-- Silver: Daily energy breakdown (active + basal) from Apple Health
--
-- Source: health_dw.bronze.stg_apple_health_active_energy_burned,
--         health_dw.bronze.stg_apple_health_basal_energy_burned
--
-- Business key: business_key_hash (date + source_name)
-- Change detection: row_hash over energy totals
-- =============================================================================

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS health_dw.silver.daily_energy_by_source (
    sk_date             INTEGER   NOT NULL,
    date                DATE      NOT NULL,
    source_name         STRING,
    active_energy_kcal  DOUBLE,
    basal_energy_kcal   DOUBLE,
    total_energy_kcal   DOUBLE,
    active_sessions     INTEGER,
    business_key_hash   STRING    NOT NULL,
    row_hash            STRING    NOT NULL,
    load_datetime       TIMESTAMP NOT NULL,
    update_datetime     TIMESTAMP NOT NULL
)
USING DELTA;

-- COMMAND ----------

CREATE OR REPLACE TABLE health_dw.silver.daily_energy_by_source_staging AS
WITH daily_active AS (
    SELECT
        to_date(startDate) AS date,
        sourceName AS source_name,
        SUM(CAST(value AS DOUBLE)) AS active_energy_kcal,
        COUNT(*) AS active_sessions
    FROM health_dw.bronze.stg_apple_health_active_energy_burned
    WHERE startDate IS NOT NULL
    GROUP BY to_date(startDate), sourceName
),
daily_basal AS (
    SELECT
        to_date(startDate) AS date,
        sourceName AS source_name,
        SUM(CAST(value AS DOUBLE)) AS basal_energy_kcal
    FROM health_dw.bronze.stg_apple_health_basal_energy_burned
    WHERE startDate IS NOT NULL
    GROUP BY to_date(startDate), sourceName
),
combined AS (
    SELECT
        COALESCE(a.date, b.date) AS date,
        COALESCE(a.source_name, b.source_name) AS source_name,
        COALESCE(a.active_energy_kcal, 0) AS active_energy_kcal,
        COALESCE(b.basal_energy_kcal, 0) AS basal_energy_kcal,
        COALESCE(a.active_energy_kcal, 0) + COALESCE(b.basal_energy_kcal, 0) AS total_energy_kcal,
        COALESCE(a.active_sessions, 0) AS active_sessions
    FROM daily_active a
    FULL OUTER JOIN daily_basal b
        ON a.date = b.date AND a.source_name = b.source_name
)
SELECT
    year(date) * 10000 + month(date) * 100 + dayofmonth(date) AS sk_date,
    date,
    source_name,
    ROUND(active_energy_kcal, 1) AS active_energy_kcal,
    ROUND(basal_energy_kcal, 1) AS basal_energy_kcal,
    ROUND(total_energy_kcal, 1) AS total_energy_kcal,
    active_sessions,
    sha2(
        concat_ws('||',
            cast(date AS STRING),
            coalesce(source_name, '')
        ), 256
    )                                                     AS business_key_hash,
    sha2(
        concat_ws('||',
            coalesce(cast(active_energy_kcal AS STRING), ''),
            coalesce(cast(basal_energy_kcal AS STRING), ''),
            coalesce(cast(active_sessions AS STRING), '')
        ), 256
    )                                                     AS row_hash,
    current_timestamp()                                   AS load_datetime
FROM combined;

-- COMMAND ----------

MERGE INTO health_dw.silver.daily_energy_by_source AS target
USING health_dw.silver.daily_energy_by_source_staging AS source
ON target.business_key_hash = source.business_key_hash

WHEN MATCHED AND target.row_hash <> source.row_hash THEN
    UPDATE SET
        target.sk_date            = source.sk_date,
        target.date               = source.date,
        target.source_name        = source.source_name,
        target.active_energy_kcal = source.active_energy_kcal,
        target.basal_energy_kcal  = source.basal_energy_kcal,
        target.total_energy_kcal  = source.total_energy_kcal,
        target.active_sessions    = source.active_sessions,
        target.row_hash           = source.row_hash,
        target.update_datetime    = current_timestamp()

WHEN NOT MATCHED THEN
    INSERT (
        sk_date, date, source_name, active_energy_kcal, basal_energy_kcal,
        total_energy_kcal, active_sessions,
        business_key_hash, row_hash, load_datetime, update_datetime
    )
    VALUES (
        source.sk_date, source.date, source.source_name,
        source.active_energy_kcal, source.basal_energy_kcal,
        source.total_energy_kcal, source.active_sessions,
        source.business_key_hash, source.row_hash,
        current_timestamp(), current_timestamp()
    );

-- COMMAND ----------

DROP TABLE IF EXISTS health_dw.silver.daily_energy_by_source_staging;
