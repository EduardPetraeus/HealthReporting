-- merge_oura_daily_resilience.sql
-- Per-source merge: Oura (API + CSV) -> silver.daily_resilience
-- Business key: day (one row per day)
-- Handles both API rows (Hive year/month/day partitions) and CSV rows (day as full date).
-- API is prioritized over CSV when both exist for the same day.
-- Note: contributors is a nested STRUCT with sleep_recovery, daytime_recovery, stress.
--
-- Usage: python run_merge.py silver/merge_oura_daily_resilience.sql

CREATE OR REPLACE TABLE silver.daily_resilience__staging AS
WITH source_data AS (
    SELECT *,
        -- Resolve date from either API (year/month/day partitions) or CSV (day as full date)
        COALESCE(
            TRY_CAST(make_date(TRY_CAST(year AS INTEGER), TRY_CAST(month AS INTEGER), TRY_CAST(day AS INTEGER)) AS DATE),
            TRY_CAST(day AS DATE)
        ) AS full_date,
        -- API rows get priority (source_rank=1) over CSV (source_rank=2)
        CASE WHEN year IS NOT NULL THEN 1 ELSE 2 END AS source_rank,
        COALESCE(_ingested_at_1::TIMESTAMP, _ingested_at::TIMESTAMP) AS ingested_at
    FROM bronze.stg_oura_daily_resilience
    WHERE day IS NOT NULL
),
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY full_date
            ORDER BY source_rank ASC, ingested_at DESC
        ) AS rn
    FROM source_data
    WHERE full_date IS NOT NULL
)
SELECT
    (year(full_date) * 10000 + month(full_date) * 100 + day(full_date))::INTEGER AS sk_date,
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
