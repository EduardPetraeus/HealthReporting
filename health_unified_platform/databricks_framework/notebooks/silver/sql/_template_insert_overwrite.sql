-- =============================================================================
-- _template_insert_overwrite.sql
-- Template for silver entities that use load_mode: insert_overwrite
--
-- Use this pattern when:
--   - The source has no reliable incremental key (no record_id / updated_at)
--   - A daily full reload of one source is acceptable
--   - You still want other sources' rows in the same silver table to be untouched
--
-- Delta Lake's REPLACE WHERE replaces only the rows matching the predicate,
-- so a full reload of apple_health never deletes oura rows.
--
-- Template variables:
--   {source_system}  — e.g. apple_health
--   {bronze_table}   — e.g. health_dw.bronze.stg_apple_health_heart_rate
--   {silver_table}   — e.g. health_dw.silver.heart_rate
-- =============================================================================

-- Create target table on first run (copy schema from the relevant entity SQL).
CREATE TABLE IF NOT EXISTS {silver_table} (
    source_system   STRING    NOT NULL,
    record_id       STRING    NOT NULL,
    recorded_at     TIMESTAMP,
    heart_rate_bpm  DOUBLE,
    _updated_at     TIMESTAMP
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- Replace only the rows owned by this source system.
-- Rows from other source systems are preserved untouched.
INSERT INTO {silver_table}
REPLACE WHERE source_system = '{source_system}'
SELECT
    source_system,
    record_id,
    recorded_at,
    CAST(heart_rate_bpm AS DOUBLE) AS heart_rate_bpm,
    current_timestamp()            AS _updated_at
FROM {bronze_table}
WHERE source_system = '{source_system}'
