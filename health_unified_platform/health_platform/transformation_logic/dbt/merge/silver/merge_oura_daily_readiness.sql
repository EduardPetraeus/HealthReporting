-- merge_oura_daily_readiness.sql
-- Per-source merge: Oura API -> silver.daily_readiness
-- Business key: day (one row per day)
-- Note: day col = hive partition day-of-month. Full date reconstructed from year/month/day.
--
-- Usage: python run_merge.py silver/merge_oura_daily_readiness.sql

CREATE OR REPLACE TABLE silver.daily_readiness__staging AS
WITH deduped AS (
    SELECT *,
        make_date(year::INTEGER, month::VARCHAR::INTEGER, day::VARCHAR::INTEGER) AS full_date,
        ROW_NUMBER() OVER (
            PARTITION BY year, month, day
            ORDER BY _ingested_at_1 DESC
        ) AS rn
    FROM bronze.stg_oura_daily_readiness
    WHERE day IS NOT NULL
)
SELECT
    (year::INTEGER * 10000 + month::VARCHAR::INTEGER * 100 + day::VARCHAR::INTEGER)::INTEGER AS sk_date,
    full_date                                              AS day,
    score::INTEGER                                         AS readiness_score,
    timestamp::TIMESTAMP                                   AS timestamp,
    temperature_deviation::DOUBLE                          AS temperature_deviation,
    temperature_trend_deviation::DOUBLE                    AS temperature_trend_deviation,
    contributors.activity_balance::INTEGER                 AS contributor_activity_balance,
    contributors.body_temperature::INTEGER                 AS contributor_body_temperature,
    contributors.hrv_balance::INTEGER                      AS contributor_hrv_balance,
    contributors.previous_day_activity::INTEGER            AS contributor_previous_day_activity,
    contributors.previous_night::INTEGER                   AS contributor_previous_night,
    contributors.recovery_index::INTEGER                   AS contributor_recovery_index,
    contributors.resting_heart_rate::INTEGER               AS contributor_resting_heart_rate,
    contributors.sleep_balance::INTEGER                    AS contributor_sleep_balance,
    contributors.sleep_regularity::INTEGER                 AS contributor_sleep_regularity,
    md5(full_date::VARCHAR)                                AS business_key_hash,
    md5(
        coalesce(cast(score AS VARCHAR), '')                                   || '||' ||
        coalesce(cast(temperature_deviation AS VARCHAR), '')                   || '||' ||
        coalesce(cast(temperature_trend_deviation AS VARCHAR), '')             || '||' ||
        coalesce(cast(contributors.activity_balance AS VARCHAR), '')           || '||' ||
        coalesce(cast(contributors.body_temperature AS VARCHAR), '')           || '||' ||
        coalesce(cast(contributors.hrv_balance AS VARCHAR), '')                || '||' ||
        coalesce(cast(contributors.previous_day_activity AS VARCHAR), '')      || '||' ||
        coalesce(cast(contributors.previous_night AS VARCHAR), '')             || '||' ||
        coalesce(cast(contributors.recovery_index AS VARCHAR), '')             || '||' ||
        coalesce(cast(contributors.resting_heart_rate AS VARCHAR), '')         || '||' ||
        coalesce(cast(contributors.sleep_balance AS VARCHAR), '')              || '||' ||
        coalesce(cast(contributors.sleep_regularity AS VARCHAR), '')
    )                                                      AS row_hash,
    current_timestamp                                      AS load_datetime
FROM deduped WHERE rn = 1;

MERGE INTO silver.daily_readiness AS target
USING silver.daily_readiness__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN UPDATE SET
    sk_date                           = src.sk_date,
    day                               = src.day,
    readiness_score                   = src.readiness_score,
    timestamp                         = src.timestamp,
    temperature_deviation             = src.temperature_deviation,
    temperature_trend_deviation       = src.temperature_trend_deviation,
    contributor_activity_balance      = src.contributor_activity_balance,
    contributor_body_temperature      = src.contributor_body_temperature,
    contributor_hrv_balance           = src.contributor_hrv_balance,
    contributor_previous_day_activity = src.contributor_previous_day_activity,
    contributor_previous_night        = src.contributor_previous_night,
    contributor_recovery_index        = src.contributor_recovery_index,
    contributor_resting_heart_rate    = src.contributor_resting_heart_rate,
    contributor_sleep_balance         = src.contributor_sleep_balance,
    contributor_sleep_regularity      = src.contributor_sleep_regularity,
    row_hash                          = src.row_hash,
    update_datetime                   = current_timestamp

WHEN NOT MATCHED THEN INSERT (
    sk_date, day, readiness_score, timestamp,
    temperature_deviation, temperature_trend_deviation,
    contributor_activity_balance, contributor_body_temperature, contributor_hrv_balance,
    contributor_previous_day_activity, contributor_previous_night, contributor_recovery_index,
    contributor_resting_heart_rate, contributor_sleep_balance, contributor_sleep_regularity,
    business_key_hash, row_hash, load_datetime, update_datetime
) VALUES (
    src.sk_date, src.day, src.readiness_score, src.timestamp,
    src.temperature_deviation, src.temperature_trend_deviation,
    src.contributor_activity_balance, src.contributor_body_temperature, src.contributor_hrv_balance,
    src.contributor_previous_day_activity, src.contributor_previous_night, src.contributor_recovery_index,
    src.contributor_resting_heart_rate, src.contributor_sleep_balance, src.contributor_sleep_regularity,
    src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
);

DROP TABLE IF EXISTS silver.daily_readiness__staging;
