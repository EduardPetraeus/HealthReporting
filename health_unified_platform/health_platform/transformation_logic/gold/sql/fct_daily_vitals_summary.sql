-- =============================================================================
-- fct_daily_vitals_summary.sql
-- Gold fact: wide daily vitals summary across multiple silver tables
--
-- Target: gold.fct_daily_vitals_summary (DuckDB local)
-- Materialization: CREATE OR REPLACE VIEW (always fresh from silver)
-- Source: silver.daily_spo2, silver.blood_pressure, silver.resting_heart_rate,
--         silver.water_intake, silver.daily_walking_gait, silver.mindful_session
-- Grain: one row per day
--
-- Output columns:
--   sk_date                INTEGER — FK to dim_date
--   day                    DATE    — calendar day
--   resting_hr_bpm         DOUBLE  — resting heart rate
--   avg_spo2_pct           DOUBLE  — average SpO2 percentage
--   avg_systolic           DOUBLE  — average systolic blood pressure
--   avg_diastolic          DOUBLE  — average diastolic blood pressure
--   avg_pulse              DOUBLE  — average pulse from BP readings
--   bp_reading_count       BIGINT  — number of BP readings
--   total_water_ml         DOUBLE  — total water intake in ml
--   water_reading_count    BIGINT  — number of water intake entries
--   avg_walking_speed      DOUBLE  — average walking speed
--   avg_step_length        DOUBLE  — average step length
--   avg_walking_asymmetry  DOUBLE  — average walking asymmetry
--   avg_double_support     DOUBLE  — average double support time
--   walking_steadiness     VARCHAR — walking steadiness classification
--   mindful_session_count  BIGINT  — number of mindful sessions
--   total_mindful_minutes  DOUBLE  — total mindfulness duration in minutes
-- =============================================================================

CREATE OR REPLACE VIEW gold.fct_daily_vitals_summary AS

WITH daily_rhr AS (
    SELECT sk_date, date AS day, resting_hr_bpm
    FROM silver.resting_heart_rate
),

daily_spo2 AS (
    SELECT
        sk_date,
        CAST(timestamp AS DATE) AS day,
        ROUND(AVG(spo2), 1) AS avg_spo2_pct
    FROM silver.blood_oxygen
    WHERE spo2 > 0
    GROUP BY sk_date, CAST(timestamp AS DATE)
),

daily_bp AS (
    SELECT
        sk_date,
        CAST(datetime AS DATE)          AS day,
        ROUND(AVG(systolic), 0)         AS avg_systolic,
        ROUND(AVG(diastolic), 0)        AS avg_diastolic,
        ROUND(AVG(pulse), 0)            AS avg_pulse,
        COUNT(*)                        AS bp_reading_count
    FROM silver.blood_pressure
    WHERE systolic > 0
    GROUP BY sk_date, CAST(datetime AS DATE)
),

daily_water AS (
    SELECT
        sk_date,
        CAST(timestamp AS DATE)         AS day,
        ROUND(SUM(water_ml), 0)         AS total_water_ml,
        COUNT(*)                        AS water_reading_count
    FROM silver.water_intake
    GROUP BY sk_date, CAST(timestamp AS DATE)
),

daily_gait AS (
    SELECT
        sk_date,
        day,
        avg_walking_speed,
        avg_step_length,
        avg_walking_asymmetry,
        avg_double_support_pct          AS avg_double_support,
        walking_steadiness
    FROM silver.daily_walking_gait
),

daily_mindful AS (
    SELECT
        sk_date,
        CAST(start_timestamp AS DATE)   AS day,
        COUNT(*)                        AS mindful_session_count,
        ROUND(SUM(duration_seconds) / 60.0, 1) AS total_mindful_minutes
    FROM silver.mindful_session
    GROUP BY sk_date, CAST(start_timestamp AS DATE)
),

-- Build a spine of all dates present in any source
date_spine AS (
    SELECT DISTINCT sk_date, day FROM daily_rhr
    UNION
    SELECT DISTINCT sk_date, day FROM daily_spo2
    UNION
    SELECT DISTINCT sk_date, day FROM daily_bp
    UNION
    SELECT DISTINCT sk_date, day FROM daily_water
    UNION
    SELECT DISTINCT sk_date, day FROM daily_gait
    UNION
    SELECT DISTINCT sk_date, day FROM daily_mindful
)

SELECT
    ds.sk_date,
    ds.day,
    rhr.resting_hr_bpm,
    spo2.avg_spo2_pct,
    bp.avg_systolic,
    bp.avg_diastolic,
    bp.avg_pulse,
    bp.bp_reading_count,
    w.total_water_ml,
    w.water_reading_count,
    g.avg_walking_speed,
    g.avg_step_length,
    g.avg_walking_asymmetry,
    g.avg_double_support,
    g.walking_steadiness,
    m.mindful_session_count,
    m.total_mindful_minutes

FROM date_spine ds
LEFT JOIN daily_rhr rhr       ON ds.sk_date = rhr.sk_date
LEFT JOIN daily_spo2 spo2     ON ds.sk_date = spo2.sk_date
LEFT JOIN daily_bp bp         ON ds.sk_date = bp.sk_date
LEFT JOIN daily_water w       ON ds.sk_date = w.sk_date
LEFT JOIN daily_gait g        ON ds.sk_date = g.sk_date
LEFT JOIN daily_mindful m     ON ds.sk_date = m.sk_date

ORDER BY ds.day DESC;
