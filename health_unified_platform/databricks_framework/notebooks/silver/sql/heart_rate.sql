-- =============================================================================
-- heart_rate.sql
-- Silver merge for heart rate data
--
-- Template variables (substituted by silver_runner.py):
--   {source_system}  — source identifier, e.g. apple_health | oura
--   {bronze_table}   — fully-qualified bronze table
--                      e.g. health_dw.bronze.stg_apple_health_heart_rate
--   {silver_table}   — fully-qualified silver table
--                      e.g. health_dw.silver.heart_rate
--
-- Shared by multiple sources writing to the same silver table.
-- This works because all source connectors normalize their bronze output
-- to the same column names before writing to parquet:
--   record_id       STRING      — unique record ID per source (UUID or hash)
--   recorded_at     TIMESTAMP   — when the heart rate reading was taken
--   heart_rate_bpm  DOUBLE      — heart rate in beats per minute
--   source_system   STRING      — injected by bronze_autoloader.py
--   _ingested_at    TIMESTAMP   — injected by bronze_autoloader.py
--
-- Unique key: (source_system, record_id)
-- Source isolation: the USING subquery filters on source_system, so a full
-- reload of one source (e.g. oura) never touches another source's rows.
-- =============================================================================

-- Create the target table on first run; MERGE INTO requires it to exist.
-- Schema is additive: ALTER TABLE to add columns rather than recreating.
CREATE TABLE IF NOT EXISTS {silver_table} (
    source_system   STRING    NOT NULL COMMENT 'Origin system (apple_health, oura, …)',
    record_id       STRING    NOT NULL COMMENT 'Unique record ID within the source system',
    recorded_at     TIMESTAMP          COMMENT 'When the heart rate reading was taken',
    heart_rate_bpm  DOUBLE             COMMENT 'Heart rate in beats per minute',
    _updated_at     TIMESTAMP          COMMENT 'Last time this row was written by the pipeline'
)
USING DELTA
COMMENT 'Silver heart rate readings unified across all source systems.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
);

MERGE INTO {silver_table} AS target
USING (
    SELECT
        source_system,
        record_id,
        recorded_at,
        CAST(heart_rate_bpm AS DOUBLE) AS heart_rate_bpm,
        current_timestamp()            AS _updated_at
    FROM {bronze_table}
    WHERE source_system = '{source_system}'
) AS source
ON  target.source_system = source.source_system
AND target.record_id     = source.record_id

WHEN MATCHED THEN
    UPDATE SET
        target.heart_rate_bpm = source.heart_rate_bpm,
        target._updated_at    = source._updated_at

WHEN NOT MATCHED THEN
    INSERT (
        source_system,
        record_id,
        recorded_at,
        heart_rate_bpm,
        _updated_at
    )
    VALUES (
        source.source_system,
        source.record_id,
        source.recorded_at,
        source.heart_rate_bpm,
        source._updated_at
    )
