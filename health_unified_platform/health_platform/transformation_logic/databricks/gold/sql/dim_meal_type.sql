-- =============================================================================
-- dim_meal_type.sql
-- Gold dimension: meal type classification for nutrition analysis
--
-- Target: health_dw.gold.dim_meal_type (Databricks)
-- Materialization: CREATE OR REPLACE TABLE (static reference data)
-- =============================================================================

CREATE OR REPLACE TABLE {target} (
    sk_meal_type       INTEGER,
    meal_type          STRING,
    meal_display_name  STRING,
    meal_order         INTEGER,
    is_main_meal       BOOLEAN
);

INSERT OVERWRITE {target}
VALUES
    (1, 'breakfast', 'Breakfast', 1, true),
    (2, 'lunch',     'Lunch',     2, true),
    (3, 'dinner',    'Dinner',    3, true),
    (4, 'snack',     'Snack',     4, false)
