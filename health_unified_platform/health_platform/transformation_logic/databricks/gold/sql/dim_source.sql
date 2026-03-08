-- =============================================================================
-- dim_source.sql
-- Gold dimension: data source classification across all silver tables
--
-- Target: health_dw.gold.dim_source (Databricks)
-- Materialization: CREATE OR REPLACE TABLE (rebuild when new sources appear)
-- =============================================================================

CREATE OR REPLACE TABLE {target} AS

WITH all_sources AS (
    SELECT DISTINCT source_name FROM health_dw.silver.heart_rate WHERE source_name IS NOT NULL
    UNION
    SELECT DISTINCT source_name FROM health_dw.silver.resting_heart_rate WHERE source_name IS NOT NULL
    UNION
    SELECT DISTINCT source_name FROM health_dw.silver.step_count WHERE source_name IS NOT NULL
    UNION
    SELECT DISTINCT source_name FROM health_dw.silver.water_intake WHERE source_name IS NOT NULL
    UNION
    SELECT DISTINCT source FROM health_dw.silver.blood_oxygen WHERE source IS NOT NULL
    UNION
    SELECT DISTINCT source FROM health_dw.silver.workout WHERE source IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (ORDER BY source_name)            AS sk_source,
    source_name,
    CASE source_name
        WHEN 'oura'         THEN 'Oura Ring'
        WHEN 'apple_health' THEN 'Apple Health'
        WHEN 'lifesum'      THEN 'Lifesum'
        WHEN 'withings'     THEN 'Withings'
        WHEN 'manual'       THEN 'Manual Entry'
        WHEN 'lab'          THEN 'Lab Test'
        ELSE REPLACE(source_name, '_', ' ')
    END                                                 AS source_display_name,
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
    END                                                 AS source_type,
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
    END                                                 AS data_collection_method

FROM all_sources
ORDER BY source_name
