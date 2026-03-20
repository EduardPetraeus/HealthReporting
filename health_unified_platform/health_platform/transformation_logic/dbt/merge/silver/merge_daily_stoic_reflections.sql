-- merge_daily_stoic_reflections.sql
-- Per-source merge: daily-stoic DuckDB -> silver.daily_stoic_reflections
-- Business key: date (one reflection per day)
-- Daily reflection/journal entries from the Daily Stoic app.
--
-- Usage: python run_merge.py silver/merge_daily_stoic_reflections.sql

CREATE TABLE IF NOT EXISTS silver.daily_stoic_reflections (
    sk_date INTEGER,
    day DATE,
    reflection_text VARCHAR,
    updated_at TIMESTAMP,
    business_key_hash VARCHAR,
    row_hash VARCHAR,
    load_datetime TIMESTAMP,
    update_datetime TIMESTAMP
);

CREATE OR REPLACE TABLE silver.daily_stoic_reflections__staging AS
WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY date
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM bronze.stg_daily_stoic_reflections
    WHERE date IS NOT NULL
)
SELECT
    CAST(REPLACE(date, '-', '') AS INTEGER)    AS sk_date,
    CAST(date AS DATE)                         AS day,
    text                                       AS reflection_text,
    CAST(updated_at AS TIMESTAMP)              AS updated_at,
    md5(date)                                  AS business_key_hash,
    md5(
        coalesce(text, '')                                       || '||' ||
        coalesce(cast(updated_at AS VARCHAR), '')
    )                                          AS row_hash,
    current_timestamp                          AS load_datetime
FROM deduped WHERE rn = 1;

MERGE INTO silver.daily_stoic_reflections AS target
USING silver.daily_stoic_reflections__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN UPDATE SET
    sk_date             = src.sk_date,
    day                 = src.day,
    reflection_text     = src.reflection_text,
    updated_at          = src.updated_at,
    row_hash            = src.row_hash,
    update_datetime     = current_timestamp

WHEN NOT MATCHED THEN INSERT (
    sk_date, day, reflection_text, updated_at,
    business_key_hash, row_hash, load_datetime, update_datetime
) VALUES (
    src.sk_date, src.day, src.reflection_text, src.updated_at,
    src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
);

COMMENT ON TABLE silver.daily_stoic_reflections IS 'Daily reflection entries from the Daily Stoic app. One row per day.';
COMMENT ON COLUMN silver.daily_stoic_reflections.sk_date IS 'Surrogate date key (YYYYMMDD integer)';
COMMENT ON COLUMN silver.daily_stoic_reflections.day IS 'Calendar date';
COMMENT ON COLUMN silver.daily_stoic_reflections.reflection_text IS 'Free-text daily reflection or journal entry';
COMMENT ON COLUMN silver.daily_stoic_reflections.updated_at IS 'Timestamp when the reflection was last updated in the source';
COMMENT ON COLUMN silver.daily_stoic_reflections.business_key_hash IS 'MD5 hash of the business key (date)';
COMMENT ON COLUMN silver.daily_stoic_reflections.row_hash IS 'MD5 hash of non-key columns for change detection';
COMMENT ON COLUMN silver.daily_stoic_reflections.load_datetime IS 'Timestamp when the row was first loaded into silver';
COMMENT ON COLUMN silver.daily_stoic_reflections.update_datetime IS 'Timestamp when the row was last updated in silver';

DROP TABLE IF EXISTS silver.daily_stoic_reflections__staging;
