-- merge_daily_stoic_habit_definitions.sql
-- Per-source merge: daily-stoic DuckDB -> silver.habit_definitions
-- Business key: id (SCD1 — one row per habit definition)
-- Reference table of habit definitions with metadata. Not date-grain.
--
-- Usage: python run_merge.py silver/merge_daily_stoic_habit_definitions.sql

CREATE TABLE IF NOT EXISTS silver.habit_definitions (
    habit_id VARCHAR,
    habit_name VARCHAR,
    habit_type VARCHAR,
    goal INTEGER,
    position INTEGER,
    active BOOLEAN,
    created_at TIMESTAMP,
    business_key_hash VARCHAR,
    row_hash VARCHAR,
    load_datetime TIMESTAMP,
    update_datetime TIMESTAMP
);

CREATE OR REPLACE TABLE silver.habit_definitions__staging AS
WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM bronze.stg_daily_stoic_habit_definitions
    WHERE id IS NOT NULL
)
SELECT
    id                                         AS habit_id,
    name                                       AS habit_name,
    type                                       AS habit_type,
    goal::INTEGER                              AS goal,
    position::INTEGER                          AS position,
    active::BOOLEAN                            AS active,
    CAST(created_at AS TIMESTAMP)              AS created_at,
    md5(id)                                    AS business_key_hash,
    md5(
        coalesce(name, '')                                       || '||' ||
        coalesce(type, '')                                       || '||' ||
        coalesce(cast(goal AS VARCHAR), '')                      || '||' ||
        coalesce(cast(position AS VARCHAR), '')                  || '||' ||
        coalesce(cast(active AS VARCHAR), '')                    || '||' ||
        coalesce(cast(created_at AS VARCHAR), '')
    )                                          AS row_hash,
    current_timestamp                          AS load_datetime
FROM deduped WHERE rn = 1;

MERGE INTO silver.habit_definitions AS target
USING silver.habit_definitions__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN UPDATE SET
    habit_id            = src.habit_id,
    habit_name          = src.habit_name,
    habit_type          = src.habit_type,
    goal                = src.goal,
    position            = src.position,
    active              = src.active,
    created_at          = src.created_at,
    row_hash            = src.row_hash,
    update_datetime     = current_timestamp

WHEN NOT MATCHED THEN INSERT (
    habit_id, habit_name, habit_type, goal, position, active, created_at,
    business_key_hash, row_hash, load_datetime, update_datetime
) VALUES (
    src.habit_id, src.habit_name, src.habit_type, src.goal, src.position, src.active, src.created_at,
    src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
);

COMMENT ON TABLE silver.habit_definitions IS 'Habit definitions from the Daily Stoic app. One row per habit (SCD1).';
COMMENT ON COLUMN silver.habit_definitions.habit_id IS 'Unique habit identifier from the source system';
COMMENT ON COLUMN silver.habit_definitions.habit_name IS 'Display name of the habit';
COMMENT ON COLUMN silver.habit_definitions.habit_type IS 'Habit type classification (e.g. boolean, counter)';
COMMENT ON COLUMN silver.habit_definitions.goal IS 'Target goal value for the habit';
COMMENT ON COLUMN silver.habit_definitions.position IS 'Display order position in the app';
COMMENT ON COLUMN silver.habit_definitions.active IS 'Whether the habit is currently active';
COMMENT ON COLUMN silver.habit_definitions.created_at IS 'Timestamp when the habit was created in the source';
COMMENT ON COLUMN silver.habit_definitions.business_key_hash IS 'MD5 hash of the business key (id)';
COMMENT ON COLUMN silver.habit_definitions.row_hash IS 'MD5 hash of non-key columns for change detection';
COMMENT ON COLUMN silver.habit_definitions.load_datetime IS 'Timestamp when the row was first loaded into silver';
COMMENT ON COLUMN silver.habit_definitions.update_datetime IS 'Timestamp when the row was last updated in silver';

DROP TABLE IF EXISTS silver.habit_definitions__staging;
