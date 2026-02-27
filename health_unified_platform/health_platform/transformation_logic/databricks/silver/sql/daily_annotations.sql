-- =============================================================================
-- daily_annotations.sql
-- Silver: Manually curated daily annotations (training events, incidents, travel, etc.)
--
-- Source: No bronze source — rows are inserted directly into this table as needed.
-- =============================================================================

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS health_dw.silver.daily_annotations (
    sk_date         INT     NOT NULL,
    annotation_type STRING  NOT NULL,
    annotation      STRING  NOT NULL,
    created_by      STRING,
    is_valid        BOOLEAN NOT NULL
)
USING DELTA;
