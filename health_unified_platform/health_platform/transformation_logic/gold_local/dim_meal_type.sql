-- =============================================================================
-- dim_meal_type.sql
-- Gold dimension: meal type classification for nutrition analysis
--
-- Target: gold.dim_meal_type (DuckDB local)
-- Materialization: CREATE OR REPLACE TABLE (static reference data)
-- Ported from: databricks/gold/sql/dim_meal_type.sql
-- =============================================================================

CREATE OR REPLACE TABLE gold.dim_meal_type (
    sk_meal_type       INTEGER,
    meal_type          VARCHAR,
    meal_display_name  VARCHAR,
    meal_order         INTEGER,
    is_main_meal       BOOLEAN
);

INSERT INTO gold.dim_meal_type
VALUES
    (1, 'breakfast', 'Breakfast', 1, true),
    (2, 'lunch',     'Lunch',     2, true),
    (3, 'dinner',    'Dinner',    3, true),
    (4, 'snack',     'Snack',     4, false)
