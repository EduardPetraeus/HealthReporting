-- merge_oura_daily_resilience.sql
-- Per-source merge: Oura API -> silver.daily_resilience
-- Business key: day (one row per day)
-- Note: Bronze table may contain both API rows (year/month/day Hive partitions)
--       and CSV backfill rows. API pattern (make_date) is primary.
-- Note: day col = hive partition day-of-month. Full date reconstructed from year/month/day.
-- Note: contributors is a nested JSON object with sleep_recovery, daytime_recovery, stress.
--
-- Usage: python run_merge.py silver/merge_oura_daily_resilience.sql

CREATE OR REPLACE TABLE silver.daily_resilience__staging AS
WITH deduped AS (
    SELECT *,
        make_date(year::INTEGER, month::VARCHAR::INTEGER, day::VARCHAR::INTEGER) AS full_date,
        ROW_NUMBER() OVER (
            PARTITION BY year, month, day
            ORDER BY _ingested_at_1 DESC
        ) AS rn
    FROM bronze.stg_oura_daily_resilience
    WHERE day IS NOT NULL
)
SELECT
    (year::INTEGER * 10000 + month::VARCHAR::INTEGER * 100 + day::VARCHAR::INTEGER)::INTEGER AS sk_date,
    full_date                              AS day,
    level::VARCHAR                         AS level,
    contributors.sleep_recovery::DOUBLE    AS contributor_sleep_recovery,
    contributors.daytime_recovery::DOUBLE  AS contributor_daytime_recovery,
    contributors.stress::DOUBLE            AS contributor_stress,
    md5(full_date::VARCHAR)                AS business_key_hash,
    md5(
        coalesce(level::VARCHAR, '')                                    || '||' ||
        coalesce(cast(contributors.sleep_recovery AS VARCHAR), '')      || '||' ||
        coalesce(cast(contributors.daytime_recovery AS VARCHAR), '')    || '||' ||
        coalesce(cast(contributors.stress AS VARCHAR), '')
    )                                      AS row_hash,
    current_timestamp                      AS load_datetime
FROM deduped WHERE rn = 1;

MERGE INTO silver.daily_resilience AS target
USING silver.daily_resilience__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN UPDATE SET
    sk_date                      = src.sk_date,
    day                          = src.day,
    level                        = src.level,
    contributor_sleep_recovery   = src.contributor_sleep_recovery,
    contributor_daytime_recovery = src.contributor_daytime_recovery,
    contributor_stress           = src.contributor_stress,
    row_hash                     = src.row_hash,
    update_datetime              = current_timestamp

WHEN NOT MATCHED THEN INSERT (
    sk_date, day, level, contributor_sleep_recovery, contributor_daytime_recovery, contributor_stress,
    business_key_hash, row_hash, load_datetime, update_datetime
) VALUES (
    src.sk_date, src.day, src.level, src.contributor_sleep_recovery,
    src.contributor_daytime_recovery, src.contributor_stress,
    src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
);

DROP TABLE IF EXISTS silver.daily_resilience__staging;
