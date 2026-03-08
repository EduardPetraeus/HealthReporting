-- =============================================================================
-- fct_daily_vitals_summary.sql
-- Gold fact: wide daily vitals summary across multiple silver tables
--
-- Target: health_dw.gold.fct_daily_vitals_summary (Databricks)
-- Materialization: CREATE OR REPLACE VIEW (always fresh from silver)
-- Grain: one row per day
-- =============================================================================

CREATE OR REPLACE VIEW {target} AS

WITH daily_rhr AS (
    SELECT sk_date, date AS day, resting_hr_bpm
    FROM health_dw.silver.resting_heart_rate
),

daily_spo2 AS (
    SELECT
        sk_date,
        day,
        ROUND(spo2_avg_pct, 1)                          AS avg_spo2_pct
    FROM health_dw.silver.daily_spo2
),

daily_bp AS (
    SELECT
        sk_date,
        CAST(datetime AS DATE)                          AS day,
        ROUND(AVG(systolic), 0)                         AS avg_systolic,
        ROUND(AVG(diastolic), 0)                        AS avg_diastolic,
        ROUND(AVG(pulse), 0)                            AS avg_pulse,
        COUNT(*)                                        AS bp_reading_count
    FROM health_dw.silver.blood_pressure
    WHERE systolic > 0
    GROUP BY sk_date, CAST(datetime AS DATE)
),

daily_water AS (
    SELECT
        sk_date,
        CAST(timestamp AS DATE)                         AS day,
        ROUND(SUM(water_ml), 0)                         AS total_water_ml,
        COUNT(*)                                        AS water_reading_count
    FROM health_dw.silver.water_intake
    GROUP BY sk_date, CAST(timestamp AS DATE)
),

daily_gait AS (
    SELECT
        sk_date,
        date                                            AS day,
        walking_speed_avg_km_hr                         AS avg_walking_speed,
        walking_step_length_avg_cm                      AS avg_step_length,
        walking_asymmetry_avg_pct                       AS avg_walking_asymmetry,
        walking_double_support_avg_pct                  AS avg_double_support,
        walking_steadiness_pct                          AS walking_steadiness
    FROM health_dw.silver.daily_walking_gait
),

daily_mindful AS (
    SELECT
        sk_date,
        CAST(timestamp AS DATE)                         AS day,
        COUNT(*)                                        AS mindful_session_count,
        ROUND(SUM(duration_seconds) / 60.0, 1)          AS total_mindful_minutes
    FROM health_dw.silver.mindful_session
    GROUP BY sk_date, CAST(timestamp AS DATE)
),

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

ORDER BY ds.day DESC
