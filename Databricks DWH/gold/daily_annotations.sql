CREATE OR REPLACE VIEW health_dw.gold.daily_annotations_valid AS
SELECT
    sk_date,
    annotation_type   AS activity_type,
    annotation        AS comment
FROM workspace.silver.daily_annotations
WHERE is_valid = TRUE;