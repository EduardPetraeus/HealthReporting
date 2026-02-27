-- =============================================================================
-- vw_daily_annotations.sql
-- Gold view: valid daily annotations (training events, incidents, travel, etc.)
--
-- Source: health_dw.silver.daily_annotations
-- Filter: only rows where is_valid = TRUE
-- =============================================================================

-- COMMAND ----------

CREATE OR REPLACE VIEW health_dw.gold.vw_daily_annotations_valid AS
SELECT
    sk_date,
    annotation_type AS activity_type,
    annotation      AS comment
FROM health_dw.silver.daily_annotations
WHERE is_valid = TRUE;
