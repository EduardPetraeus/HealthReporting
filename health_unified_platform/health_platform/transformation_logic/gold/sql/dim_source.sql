-- =============================================================================
-- dim_source.sql
-- Gold dimension: data source classification across all silver tables
--
-- Target: gold.dim_source (DuckDB local)
-- Materialization: CREATE TABLE (rebuild when new sources appear)
-- Source: DISTINCT source_name values from silver tables
--
-- Output columns:
--   sk_source              INTEGER  — surrogate key
--   source_name            VARCHAR  — raw source identifier from silver
--   source_display_name    VARCHAR  — human-friendly name
--   source_type            VARCHAR  — wearable/app/scale/lab/manual
--   data_collection_method VARCHAR  — how data is collected
-- =============================================================================

CREATE OR REPLACE TABLE gold.dim_source AS

WITH all_sources AS (
    SELECT DISTINCT source_name FROM silver.heart_rate WHERE source_name IS NOT NULL
    UNION
    SELECT DISTINCT source_name FROM silver.resting_heart_rate WHERE source_name IS NOT NULL
    UNION
    SELECT DISTINCT source_name FROM silver.step_count WHERE source_name IS NOT NULL
    UNION
    SELECT DISTINCT source_name FROM silver.water_intake WHERE source_name IS NOT NULL
    UNION
    SELECT DISTINCT source FROM silver.blood_oxygen WHERE source IS NOT NULL
    UNION
    SELECT DISTINCT source FROM silver.workout WHERE source IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (ORDER BY source_name)     AS sk_source,
    source_name,
    CASE source_name
        WHEN 'oura'         THEN 'Oura Ring'
        WHEN 'apple_health' THEN 'Apple Health'
        WHEN 'lifesum'      THEN 'Lifesum'
        WHEN 'withings'     THEN 'Withings'
        WHEN 'manual'       THEN 'Manual Entry'
        WHEN 'lab'          THEN 'Lab Test'
        ELSE REPLACE(source_name, '_', ' ')
    END                                          AS source_display_name,
    CASE
        WHEN source_name IN ('oura', 'apple_health', 'apple_watch')
            THEN 'wearable'
        WHEN source_name IN ('lifesum')
            THEN 'app'
        WHEN source_name IN ('withings')
            THEN 'scale'
        WHEN source_name IN ('lab')
            THEN 'lab'
        WHEN source_name IN ('manual')
            THEN 'manual'
        ELSE 'other'
    END                                          AS source_type,
    CASE
        WHEN source_name IN ('oura', 'apple_health', 'apple_watch')
            THEN 'continuous_passive'
        WHEN source_name IN ('withings')
            THEN 'periodic_passive'
        WHEN source_name IN ('lifesum')
            THEN 'manual_entry'
        WHEN source_name IN ('lab')
            THEN 'file_export'
        ELSE 'manual_entry'
    END                                          AS data_collection_method

FROM all_sources
ORDER BY source_name;
