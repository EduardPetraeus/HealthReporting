-- dim_habit.sql
-- Gold dimension: habit catalog from daily-stoic
-- Target: gold.dim_habit (DuckDB local)

CREATE OR REPLACE TABLE gold.dim_habit AS

SELECT
    ROW_NUMBER() OVER (ORDER BY habit_id)   AS sk_habit,
    habit_id,
    habit_name,
    habit_type,
    goal,
    position,
    active
FROM silver.habit_definitions
ORDER BY position, habit_name
