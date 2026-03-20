-- fct_daily_experience.sql
-- Gold fact: unified daily experience combining focus, reflections, habits, and quotes
-- Target: gold.fct_daily_experience (DuckDB local)
-- Grain: one row per calendar date

CREATE OR REPLACE TABLE gold.fct_daily_experience AS

WITH habit_summary AS (
    SELECT
        sk_date,
        day,
        COUNT(*) AS total_habits,
        SUM(CASE WHEN done THEN 1 ELSE 0 END) AS completed_habits,
        ROUND(SUM(CASE WHEN done THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS completion_pct,
        STRING_AGG(
            CASE WHEN done THEN habit_id END, ', '
            ORDER BY habit_id
        ) AS completed_habit_ids
    FROM silver.daily_stoic_habits
    GROUP BY sk_date, day
)

SELECT
    COALESCE(h.sk_date, f.sk_date, r.sk_date, q.sk_date)   AS sk_date,
    COALESCE(h.day, f.day, r.day, q.day)                     AS day,
    f.focus_text,
    r.reflection_text,
    q.quote_text,
    q.author                                                  AS quote_author,
    h.total_habits,
    h.completed_habits,
    h.completion_pct,
    h.completed_habit_ids
FROM habit_summary h
FULL OUTER JOIN silver.daily_stoic_focus f ON h.sk_date = f.sk_date
FULL OUTER JOIN silver.daily_stoic_reflections r ON COALESCE(h.sk_date, f.sk_date) = r.sk_date
LEFT JOIN silver.stoic_quotes q ON COALESCE(h.sk_date, f.sk_date, r.sk_date) = q.sk_date
ORDER BY day DESC
