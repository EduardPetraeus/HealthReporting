-- merge_daily_stoic_habits.sql
-- Per-source merge: daily-stoic DuckDB -> silver.daily_stoic_habits
-- Business key: date + habit_id (composite — one row per habit per day)
-- Daily habit completion tracking from the Daily Stoic app.
--
-- Usage: python run_merge.py silver/merge_daily_stoic_habits.sql

CREATE TABLE IF NOT EXISTS silver.daily_stoic_habits (
    sk_date INTEGER,
    day DATE,
    habit_id VARCHAR,
    done BOOLEAN,
    toggled_at TIMESTAMP,
    business_key_hash VARCHAR,
    row_hash VARCHAR,
    load_datetime TIMESTAMP,
    update_datetime TIMESTAMP
);

CREATE OR REPLACE TABLE silver.daily_stoic_habits__staging AS
WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY date, habit_id
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM bronze.stg_daily_stoic_habits
    WHERE date IS NOT NULL
      AND habit_id IS NOT NULL
)
SELECT
    CAST(REPLACE(date, '-', '') AS INTEGER)    AS sk_date,
    CAST(date AS DATE)                         AS day,
    habit_id                                   AS habit_id,
    done::BOOLEAN                              AS done,
    CAST(toggled_at AS TIMESTAMP)              AS toggled_at,
    md5(date || '||' || habit_id)              AS business_key_hash,
    md5(
        coalesce(cast(done AS VARCHAR), '')                      || '||' ||
        coalesce(cast(toggled_at AS VARCHAR), '')
    )                                          AS row_hash,
    current_timestamp                          AS load_datetime
FROM deduped WHERE rn = 1;

MERGE INTO silver.daily_stoic_habits AS target
USING silver.daily_stoic_habits__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN UPDATE SET
    sk_date             = src.sk_date,
    day                 = src.day,
    habit_id            = src.habit_id,
    done                = src.done,
    toggled_at          = src.toggled_at,
    row_hash            = src.row_hash,
    update_datetime     = current_timestamp

WHEN NOT MATCHED THEN INSERT (
    sk_date, day, habit_id, done, toggled_at,
    business_key_hash, row_hash, load_datetime, update_datetime
) VALUES (
    src.sk_date, src.day, src.habit_id, src.done, src.toggled_at,
    src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
);

COMMENT ON TABLE silver.daily_stoic_habits IS 'Daily habit completion records from the Daily Stoic app. One row per habit per day.';
COMMENT ON COLUMN silver.daily_stoic_habits.sk_date IS 'Surrogate date key (YYYYMMDD integer)';
COMMENT ON COLUMN silver.daily_stoic_habits.day IS 'Calendar date';
COMMENT ON COLUMN silver.daily_stoic_habits.habit_id IS 'Foreign key to silver.habit_definitions';
COMMENT ON COLUMN silver.daily_stoic_habits.done IS 'Whether the habit was completed on this day';
COMMENT ON COLUMN silver.daily_stoic_habits.toggled_at IS 'Timestamp when the habit was last toggled in the source';
COMMENT ON COLUMN silver.daily_stoic_habits.business_key_hash IS 'MD5 hash of the business key (date + habit_id)';
COMMENT ON COLUMN silver.daily_stoic_habits.row_hash IS 'MD5 hash of non-key columns for change detection';
COMMENT ON COLUMN silver.daily_stoic_habits.load_datetime IS 'Timestamp when the row was first loaded into silver';
COMMENT ON COLUMN silver.daily_stoic_habits.update_datetime IS 'Timestamp when the row was last updated in silver';

DROP TABLE IF EXISTS silver.daily_stoic_habits__staging;
