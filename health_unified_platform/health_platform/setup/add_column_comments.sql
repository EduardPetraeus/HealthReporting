-- Column and Table Comments for the Health Unified Platform
-- Covers all 21 silver-layer tables, silver.metric_relationships, all agent.* tables,
-- and dimension/genetics tables: dim_marker_catalog, dim_reference_range, genetic_ancestry, genetic_traits.
-- DuckDB-compatible COMMENT ON syntax.

-- =============================================================================
-- 1. silver.blood_oxygen
-- =============================================================================
COMMENT ON TABLE silver.blood_oxygen IS 'Continuous and spot-check blood oxygen (SpO2) readings from wearable sensors. Each row is a single measurement at a point in time.';

COMMENT ON COLUMN silver.blood_oxygen.sk_date IS 'Surrogate date key in YYYYMMDD integer format. Foreign key to the date dimension.';
COMMENT ON COLUMN silver.blood_oxygen.sk_time IS 'Surrogate time key as HH:MM string. Used for time-of-day analysis and joins to time dimension.';
COMMENT ON COLUMN silver.blood_oxygen.timestamp IS 'UTC timestamp of the SpO2 measurement.';
COMMENT ON COLUMN silver.blood_oxygen.spo2 IS 'Peripheral oxygen saturation percentage. Normal range: 95-100%. Values below 90% are clinically concerning. Source: pulse oximeter or wearable optical sensor.';
COMMENT ON COLUMN silver.blood_oxygen.source IS 'Name of the device or app that produced the reading (e.g., Oura, Apple Watch).';
COMMENT ON COLUMN silver.blood_oxygen.business_key_hash IS 'MD5/SHA hash of the natural business key columns. Used for SCD tracking and deduplication.';
COMMENT ON COLUMN silver.blood_oxygen.row_hash IS 'MD5/SHA hash of all non-key columns. Used to detect changes in measure values across loads.';
COMMENT ON COLUMN silver.blood_oxygen.load_datetime IS 'Timestamp when this row was first loaded into the silver layer.';
COMMENT ON COLUMN silver.blood_oxygen.update_datetime IS 'Timestamp when this row was last updated in the silver layer.';

-- =============================================================================
-- 2. silver.blood_pressure
-- =============================================================================
COMMENT ON TABLE silver.blood_pressure IS 'Blood pressure readings with systolic, diastolic, and pulse values. Typically from a home blood pressure monitor. Each row is one measurement session.';

COMMENT ON COLUMN silver.blood_pressure.sk_date IS 'Surrogate date key in YYYYMMDD integer format.';
COMMENT ON COLUMN silver.blood_pressure.sk_time IS 'Surrogate time key as HH:MM string.';
COMMENT ON COLUMN silver.blood_pressure.datetime IS 'UTC timestamp of the blood pressure measurement.';
COMMENT ON COLUMN silver.blood_pressure.systolic IS 'Systolic blood pressure in mmHg. Normal: <120, elevated: 120-129, high stage 1: 130-139, high stage 2: >=140. Source: blood pressure monitor.';
COMMENT ON COLUMN silver.blood_pressure.diastolic IS 'Diastolic blood pressure in mmHg. Normal: <80, high stage 1: 80-89, high stage 2: >=90. Source: blood pressure monitor.';
COMMENT ON COLUMN silver.blood_pressure.pulse IS 'Pulse rate in beats per minute measured during the blood pressure reading. Resting range: 60-100 bpm.';
COMMENT ON COLUMN silver.blood_pressure.business_key_hash IS 'MD5/SHA hash of the natural business key columns.';
COMMENT ON COLUMN silver.blood_pressure.row_hash IS 'MD5/SHA hash of all non-key columns for change detection.';
COMMENT ON COLUMN silver.blood_pressure.load_datetime IS 'Timestamp when this row was first loaded into the silver layer.';
COMMENT ON COLUMN silver.blood_pressure.update_datetime IS 'Timestamp when this row was last updated in the silver layer.';

-- =============================================================================
-- 3. silver.body_temperature
-- =============================================================================
COMMENT ON TABLE silver.body_temperature IS 'Body temperature readings in degrees Celsius. Can come from wearables (skin temperature) or manual thermometer entries.';

COMMENT ON COLUMN silver.body_temperature.sk_date IS 'Surrogate date key in YYYYMMDD integer format.';
COMMENT ON COLUMN silver.body_temperature.sk_time IS 'Surrogate time key as HH:MM string.';
COMMENT ON COLUMN silver.body_temperature.timestamp IS 'UTC timestamp of the temperature measurement.';
COMMENT ON COLUMN silver.body_temperature.temperature_degc IS 'Body temperature in degrees Celsius. Normal core range: 36.1-37.2C. Skin temperature from wearables is typically 1-2C lower. Deviations can indicate illness, ovulation, or circadian phase.';
COMMENT ON COLUMN silver.body_temperature.source_name IS 'Name of the device or app that produced the reading (e.g., Oura skin temp, manual thermometer).';
COMMENT ON COLUMN silver.body_temperature.business_key_hash IS 'MD5/SHA hash of the natural business key columns.';
COMMENT ON COLUMN silver.body_temperature.row_hash IS 'MD5/SHA hash of all non-key columns for change detection.';
COMMENT ON COLUMN silver.body_temperature.load_datetime IS 'Timestamp when this row was first loaded into the silver layer.';
COMMENT ON COLUMN silver.body_temperature.update_datetime IS 'Timestamp when this row was last updated in the silver layer.';

-- =============================================================================
-- 4. silver.daily_activity
-- =============================================================================
COMMENT ON TABLE silver.daily_activity IS 'Daily activity summary from Oura Ring. One row per day with scores, calorie burn, movement breakdown, and contributor sub-scores. Primary source for movement and energy expenditure analysis.';

COMMENT ON COLUMN silver.daily_activity.sk_date IS 'Surrogate date key in YYYYMMDD integer format.';
COMMENT ON COLUMN silver.daily_activity.day IS 'Calendar date for this activity summary.';
COMMENT ON COLUMN silver.daily_activity.activity_score IS 'Oura composite activity score (0-100). Optimal: 70-100. Considers movement volume, intensity mix, and training balance.';
COMMENT ON COLUMN silver.daily_activity.timestamp IS 'UTC timestamp when the daily activity summary was finalized by Oura.';
COMMENT ON COLUMN silver.daily_activity.steps IS 'Total step count for the day. General guideline: 7000-10000 steps/day for health benefits. Source: Oura Ring accelerometer.';
COMMENT ON COLUMN silver.daily_activity.total_calories IS 'Total calories burned including basal metabolic rate and active calories. Unit: kcal.';
COMMENT ON COLUMN silver.daily_activity.active_calories IS 'Calories burned above basal metabolic rate through physical activity. Unit: kcal.';
COMMENT ON COLUMN silver.daily_activity.target_calories IS 'Personalized daily active calorie target set by Oura based on recent activity patterns. Unit: kcal.';
COMMENT ON COLUMN silver.daily_activity.average_met_minutes IS 'Average metabolic equivalent minutes across the day. 1 MET = resting energy expenditure. Higher values indicate more intense activity.';
COMMENT ON COLUMN silver.daily_activity.equivalent_walking_distance IS 'Total activity expressed as equivalent walking distance in meters. Normalizes different activity types to a common unit.';
COMMENT ON COLUMN silver.daily_activity.high_activity_time IS 'Minutes spent in high-intensity activity (>6 METs, e.g., running, intense cycling). Unit: seconds.';
COMMENT ON COLUMN silver.daily_activity.medium_activity_time IS 'Minutes spent in medium-intensity activity (3-6 METs, e.g., brisk walking, light cycling). Unit: seconds.';
COMMENT ON COLUMN silver.daily_activity.low_activity_time IS 'Minutes spent in low-intensity activity (1.5-3 METs, e.g., slow walking, light housework). Unit: seconds.';
COMMENT ON COLUMN silver.daily_activity.sedentary_time IS 'Time spent sedentary (<1.5 METs). Prolonged sedentary time (>8hrs) is associated with health risks. Unit: seconds.';
COMMENT ON COLUMN silver.daily_activity.resting_time IS 'Time spent resting or sleeping as detected by the ring. Unit: seconds.';
COMMENT ON COLUMN silver.daily_activity.non_wear_time IS 'Time the ring was not worn. High values reduce data reliability for that day. Unit: seconds.';
COMMENT ON COLUMN silver.daily_activity.high_activity_met_minutes IS 'MET-minutes accumulated during high-intensity activity periods.';
COMMENT ON COLUMN silver.daily_activity.medium_activity_met_minutes IS 'MET-minutes accumulated during medium-intensity activity periods.';
COMMENT ON COLUMN silver.daily_activity.low_activity_met_minutes IS 'MET-minutes accumulated during low-intensity activity periods.';
COMMENT ON COLUMN silver.daily_activity.sedentary_met_minutes IS 'MET-minutes accumulated during sedentary periods (baseline metabolic activity).';
COMMENT ON COLUMN silver.daily_activity.inactivity_alerts IS 'Number of inactivity alerts triggered during the day. Oura nudges movement after prolonged sitting (typically 50+ minutes).';
COMMENT ON COLUMN silver.daily_activity.target_meters IS 'Daily distance target in meters based on personalized Oura algorithm.';
COMMENT ON COLUMN silver.daily_activity.meters_to_target IS 'Remaining meters to reach the daily distance target. Negative means target exceeded.';
COMMENT ON COLUMN silver.daily_activity.contributor_meet_daily_targets IS 'Activity score contributor: how well daily movement targets were met (0-100).';
COMMENT ON COLUMN silver.daily_activity.contributor_move_every_hour IS 'Activity score contributor: regularity of hourly movement throughout the day (0-100).';
COMMENT ON COLUMN silver.daily_activity.contributor_recovery_time IS 'Activity score contributor: adequate recovery between intense sessions (0-100).';
COMMENT ON COLUMN silver.daily_activity.contributor_stay_active IS 'Activity score contributor: overall daily activity volume (0-100).';
COMMENT ON COLUMN silver.daily_activity.contributor_training_frequency IS 'Activity score contributor: consistency of training across the week (0-100).';
COMMENT ON COLUMN silver.daily_activity.contributor_training_volume IS 'Activity score contributor: total training load relative to capacity (0-100).';
COMMENT ON COLUMN silver.daily_activity.business_key_hash IS 'MD5/SHA hash of the natural business key columns.';
COMMENT ON COLUMN silver.daily_activity.row_hash IS 'MD5/SHA hash of all non-key columns for change detection.';
COMMENT ON COLUMN silver.daily_activity.load_datetime IS 'Timestamp when this row was first loaded into the silver layer.';
COMMENT ON COLUMN silver.daily_activity.update_datetime IS 'Timestamp when this row was last updated in the silver layer.';

-- =============================================================================
-- 5. silver.daily_annotations
-- =============================================================================
COMMENT ON TABLE silver.daily_annotations IS 'Manual annotations and tags applied to specific days. Used for marking events (e.g., illness, travel, medication changes) that provide context for anomaly analysis.';

COMMENT ON COLUMN silver.daily_annotations.sk_date IS 'Surrogate date key in YYYYMMDD integer format.';
COMMENT ON COLUMN silver.daily_annotations.annotation_type IS 'Category of annotation (e.g., illness, travel, medication, lifestyle_change, event). Used for filtering and grouping.';
COMMENT ON COLUMN silver.daily_annotations.annotation IS 'Free-text description of the annotation or event.';
COMMENT ON COLUMN silver.daily_annotations.created_by IS 'Who created the annotation: user (manual entry) or agent (AI-generated).';
COMMENT ON COLUMN silver.daily_annotations.is_valid IS 'Whether this annotation is currently valid/active. FALSE if retracted or superseded.';

-- =============================================================================
-- 6. silver.daily_energy_by_source
-- =============================================================================
COMMENT ON TABLE silver.daily_energy_by_source IS 'Daily energy expenditure broken down by source device/app. Allows comparison of calorie estimates across different trackers and activity types.';

COMMENT ON COLUMN silver.daily_energy_by_source.sk_date IS 'Surrogate date key in YYYYMMDD integer format.';
COMMENT ON COLUMN silver.daily_energy_by_source.date IS 'Calendar date for this energy summary.';
COMMENT ON COLUMN silver.daily_energy_by_source.source_name IS 'Name of the source device or app reporting energy data (e.g., Apple Watch, Oura, Strava).';
COMMENT ON COLUMN silver.daily_energy_by_source.active_energy_kcal IS 'Active energy burned as reported by this source. Unit: kilocalories. Excludes basal metabolic rate.';
COMMENT ON COLUMN silver.daily_energy_by_source.basal_energy_kcal IS 'Basal (resting) energy expenditure as estimated by this source. Unit: kilocalories.';
COMMENT ON COLUMN silver.daily_energy_by_source.total_energy_kcal IS 'Total energy expenditure (active + basal) as reported by this source. Unit: kilocalories.';
COMMENT ON COLUMN silver.daily_energy_by_source.active_sessions IS 'Number of distinct active/workout sessions recorded by this source on this day.';
COMMENT ON COLUMN silver.daily_energy_by_source.business_key_hash IS 'MD5/SHA hash of the natural business key columns.';
COMMENT ON COLUMN silver.daily_energy_by_source.row_hash IS 'MD5/SHA hash of all non-key columns for change detection.';
COMMENT ON COLUMN silver.daily_energy_by_source.load_datetime IS 'Timestamp when this row was first loaded into the silver layer.';
COMMENT ON COLUMN silver.daily_energy_by_source.update_datetime IS 'Timestamp when this row was last updated in the silver layer.';

-- =============================================================================
-- 7. silver.daily_meal
-- =============================================================================
COMMENT ON TABLE silver.daily_meal IS 'Nutritional data for individual food items consumed per meal. Sourced from food logging apps (e.g., MyFitnessPal, Cronometer). Granularity: one row per food item per meal.';

COMMENT ON COLUMN silver.daily_meal.sk_date IS 'Surrogate date key in YYYYMMDD integer format.';
COMMENT ON COLUMN silver.daily_meal.date IS 'Calendar date when the meal was consumed.';
COMMENT ON COLUMN silver.daily_meal.meal_type IS 'Meal category: breakfast, lunch, dinner, or snack.';
COMMENT ON COLUMN silver.daily_meal.food_item IS 'Name or description of the food item consumed.';
COMMENT ON COLUMN silver.daily_meal.brand IS 'Brand name of the food product, if applicable. NULL for unbranded/generic foods.';
COMMENT ON COLUMN silver.daily_meal.serving_name IS 'Description of the serving unit (e.g., cup, slice, piece, 100g).';
COMMENT ON COLUMN silver.daily_meal.amount IS 'Number of servings consumed (e.g., 1.5 cups, 2 slices).';
COMMENT ON COLUMN silver.daily_meal.amount_in_grams IS 'Weight of the consumed food in grams. Standardized for cross-food comparison.';
COMMENT ON COLUMN silver.daily_meal.calories IS 'Energy content of the consumed portion. Unit: kilocalories.';
COMMENT ON COLUMN silver.daily_meal.carbs IS 'Total carbohydrate content. Unit: grams. Recommended: 45-65% of total calories.';
COMMENT ON COLUMN silver.daily_meal.carbs_fiber IS 'Dietary fiber content. Unit: grams. Recommended intake: 25-30g/day.';
COMMENT ON COLUMN silver.daily_meal.carbs_sugar IS 'Sugar content (simple carbohydrates). Unit: grams. WHO recommends <25g added sugar/day.';
COMMENT ON COLUMN silver.daily_meal.cholesterol IS 'Cholesterol content. Unit: milligrams. Dietary guideline: <300mg/day.';
COMMENT ON COLUMN silver.daily_meal.fat IS 'Total fat content. Unit: grams. Recommended: 20-35% of total calories.';
COMMENT ON COLUMN silver.daily_meal.fat_saturated IS 'Saturated fat content. Unit: grams. Recommended: <10% of total calories.';
COMMENT ON COLUMN silver.daily_meal.fat_unsaturated IS 'Unsaturated fat content (mono + polyunsaturated). Unit: grams. Generally healthier fat type.';
COMMENT ON COLUMN silver.daily_meal.potassium IS 'Potassium content. Unit: milligrams. Adequate intake: ~2600-3400mg/day. Important for blood pressure regulation.';
COMMENT ON COLUMN silver.daily_meal.protein IS 'Protein content. Unit: grams. Recommended: 0.8-2.0g per kg body weight depending on activity level.';
COMMENT ON COLUMN silver.daily_meal.sodium IS 'Sodium content. Unit: milligrams. Recommended: <2300mg/day. Excess linked to hypertension.';
COMMENT ON COLUMN silver.daily_meal.business_key_hash IS 'MD5/SHA hash of the natural business key columns.';
COMMENT ON COLUMN silver.daily_meal.row_hash IS 'MD5/SHA hash of all non-key columns for change detection.';
COMMENT ON COLUMN silver.daily_meal.load_datetime IS 'Timestamp when this row was first loaded into the silver layer.';
COMMENT ON COLUMN silver.daily_meal.update_datetime IS 'Timestamp when this row was last updated in the silver layer.';

-- =============================================================================
-- 8. silver.daily_readiness
-- =============================================================================
COMMENT ON TABLE silver.daily_readiness IS 'Oura daily readiness assessment. Composite score reflecting recovery status and capacity for physical and mental strain. One row per day.';

COMMENT ON COLUMN silver.daily_readiness.sk_date IS 'Surrogate date key in YYYYMMDD integer format.';
COMMENT ON COLUMN silver.daily_readiness.day IS 'Calendar date for this readiness assessment.';
COMMENT ON COLUMN silver.daily_readiness.readiness_score IS 'Oura composite readiness score (0-100). Optimal: 70-100. Below 60 suggests prioritizing recovery. Derived from sleep, HRV, temperature, and activity balance.';
COMMENT ON COLUMN silver.daily_readiness.timestamp IS 'UTC timestamp when the readiness score was calculated by Oura.';
COMMENT ON COLUMN silver.daily_readiness.temperature_deviation IS 'Deviation from personal baseline body temperature in degrees Celsius. Positive values may indicate illness or hormonal changes. Normal fluctuation: +/-0.5C.';
COMMENT ON COLUMN silver.daily_readiness.temperature_trend_deviation IS 'Trend-adjusted temperature deviation, filtering out day-to-day noise. Sustained deviations (>1C for 2+ days) are more clinically meaningful.';
COMMENT ON COLUMN silver.daily_readiness.contributor_activity_balance IS 'Readiness contributor: balance between recent activity load and recovery capacity (0-100).';
COMMENT ON COLUMN silver.daily_readiness.contributor_body_temperature IS 'Readiness contributor: body temperature stability relative to personal baseline (0-100).';
COMMENT ON COLUMN silver.daily_readiness.contributor_hrv_balance IS 'Readiness contributor: heart rate variability relative to personal baseline (0-100). Key autonomic nervous system indicator.';
COMMENT ON COLUMN silver.daily_readiness.contributor_previous_day_activity IS 'Readiness contributor: impact of previous day activity intensity on current recovery (0-100).';
COMMENT ON COLUMN silver.daily_readiness.contributor_previous_night IS 'Readiness contributor: quality and duration of the previous night sleep (0-100).';
COMMENT ON COLUMN silver.daily_readiness.contributor_recovery_index IS 'Readiness contributor: how quickly resting heart rate stabilized during sleep (0-100). Faster stabilization = better recovery.';
COMMENT ON COLUMN silver.daily_readiness.contributor_resting_heart_rate IS 'Readiness contributor: resting heart rate relative to personal baseline (0-100). Lower than baseline is favorable.';
COMMENT ON COLUMN silver.daily_readiness.contributor_sleep_balance IS 'Readiness contributor: sleep debt accumulation over the past 2 weeks (0-100).';
COMMENT ON COLUMN silver.daily_readiness.contributor_sleep_regularity IS 'Readiness contributor: consistency of sleep/wake timing over recent days (0-100).';
COMMENT ON COLUMN silver.daily_readiness.business_key_hash IS 'MD5/SHA hash of the natural business key columns.';
COMMENT ON COLUMN silver.daily_readiness.row_hash IS 'MD5/SHA hash of all non-key columns for change detection.';
COMMENT ON COLUMN silver.daily_readiness.load_datetime IS 'Timestamp when this row was first loaded into the silver layer.';
COMMENT ON COLUMN silver.daily_readiness.update_datetime IS 'Timestamp when this row was last updated in the silver layer.';

-- =============================================================================
-- 9. silver.daily_sleep
-- =============================================================================
COMMENT ON TABLE silver.daily_sleep IS 'Oura daily sleep score and contributor breakdown. One row per day summarizing sleep quality from the previous night. Key input to readiness and recovery analysis.';

COMMENT ON COLUMN silver.daily_sleep.sk_date IS 'Surrogate date key in YYYYMMDD integer format.';
COMMENT ON COLUMN silver.daily_sleep.day IS 'Calendar date this sleep score applies to (the morning after sleep).';
COMMENT ON COLUMN silver.daily_sleep.sleep_score IS 'Oura composite sleep score (0-100). Optimal: 75-100. Below 60 indicates poor sleep. Derived from duration, efficiency, staging, timing, and restfulness.';
COMMENT ON COLUMN silver.daily_sleep.timestamp IS 'UTC timestamp when the sleep score was finalized by Oura.';
COMMENT ON COLUMN silver.daily_sleep.contributor_deep_sleep IS 'Sleep score contributor: amount of deep (N3/slow-wave) sleep relative to personal needs (0-100). Adults need ~1-2 hours. Critical for physical recovery and memory consolidation.';
COMMENT ON COLUMN silver.daily_sleep.contributor_efficiency IS 'Sleep score contributor: percentage of time in bed actually spent sleeping (0-100). Optimal: >85% sleep efficiency.';
COMMENT ON COLUMN silver.daily_sleep.contributor_latency IS 'Sleep score contributor: time to fall asleep (0-100). Optimal sleep onset: 10-20 minutes. Too fast (<5min) may indicate sleep deprivation.';
COMMENT ON COLUMN silver.daily_sleep.contributor_rem_sleep IS 'Sleep score contributor: amount of REM sleep relative to personal needs (0-100). Adults need ~1.5-2 hours. Important for emotional regulation and learning.';
COMMENT ON COLUMN silver.daily_sleep.contributor_restfulness IS 'Sleep score contributor: sleep continuity and absence of disturbances (0-100). Fewer wake-ups and movement = higher restfulness.';
COMMENT ON COLUMN silver.daily_sleep.contributor_timing IS 'Sleep score contributor: alignment of sleep midpoint with optimal circadian window (0-100). Consistent timing supports circadian health.';
COMMENT ON COLUMN silver.daily_sleep.contributor_total_sleep IS 'Sleep score contributor: total sleep duration relative to personal needs (0-100). Most adults need 7-9 hours.';
COMMENT ON COLUMN silver.daily_sleep.business_key_hash IS 'MD5/SHA hash of the natural business key columns.';
COMMENT ON COLUMN silver.daily_sleep.row_hash IS 'MD5/SHA hash of all non-key columns for change detection.';
COMMENT ON COLUMN silver.daily_sleep.load_datetime IS 'Timestamp when this row was first loaded into the silver layer.';
COMMENT ON COLUMN silver.daily_sleep.update_datetime IS 'Timestamp when this row was last updated in the silver layer.';

-- =============================================================================
-- 10. silver.daily_spo2
-- =============================================================================
COMMENT ON TABLE silver.daily_spo2 IS 'Oura daily average blood oxygen and breathing disturbance index. One row per night summarizing nocturnal respiratory health. Useful for detecting sleep apnea patterns.';

COMMENT ON COLUMN silver.daily_spo2.sk_date IS 'Surrogate date key in YYYYMMDD integer format.';
COMMENT ON COLUMN silver.daily_spo2.day IS 'Calendar date (the morning after the measured night).';
COMMENT ON COLUMN silver.daily_spo2.spo2_avg_pct IS 'Average nocturnal SpO2 percentage. Normal: 95-100%. Values consistently below 93% warrant medical evaluation for sleep-disordered breathing.';
COMMENT ON COLUMN silver.daily_spo2.breathing_disturbance_index IS 'Number of breathing disturbances per hour of sleep. Normal: <5. Mild: 5-15. Moderate: 15-30. Severe: >30. Proxy for apnea-hypopnea index (AHI). Source: Oura Ring.';
COMMENT ON COLUMN silver.daily_spo2.business_key_hash IS 'MD5/SHA hash of the natural business key columns.';
COMMENT ON COLUMN silver.daily_spo2.row_hash IS 'MD5/SHA hash of all non-key columns for change detection.';
COMMENT ON COLUMN silver.daily_spo2.load_datetime IS 'Timestamp when this row was first loaded into the silver layer.';
COMMENT ON COLUMN silver.daily_spo2.update_datetime IS 'Timestamp when this row was last updated in the silver layer.';

-- =============================================================================
-- 11. silver.daily_stress
-- =============================================================================
COMMENT ON TABLE silver.daily_stress IS 'Oura daily stress and recovery summary. Derived from daytime HRV analysis. One row per day indicating overall autonomic nervous system balance.';

COMMENT ON COLUMN silver.daily_stress.sk_date IS 'Surrogate date key in YYYYMMDD integer format.';
COMMENT ON COLUMN silver.daily_stress.day IS 'Calendar date for this stress assessment.';
COMMENT ON COLUMN silver.daily_stress.day_summary IS 'Qualitative summary of the day stress profile (e.g., restored, normal, stressed). Based on the ratio of high-stress to high-recovery time.';
COMMENT ON COLUMN silver.daily_stress.stress_high IS 'Total time in high-stress state during the day. Unit: seconds. Driven by low HRV periods indicating sympathetic dominance.';
COMMENT ON COLUMN silver.daily_stress.recovery_high IS 'Total time in high-recovery state during the day. Unit: seconds. Driven by high HRV periods indicating parasympathetic dominance.';
COMMENT ON COLUMN silver.daily_stress.business_key_hash IS 'MD5/SHA hash of the natural business key columns.';
COMMENT ON COLUMN silver.daily_stress.row_hash IS 'MD5/SHA hash of all non-key columns for change detection.';
COMMENT ON COLUMN silver.daily_stress.load_datetime IS 'Timestamp when this row was first loaded into the silver layer.';
COMMENT ON COLUMN silver.daily_stress.update_datetime IS 'Timestamp when this row was last updated in the silver layer.';

-- =============================================================================
-- 12. silver.daily_walking_gait
-- =============================================================================
COMMENT ON TABLE silver.daily_walking_gait IS 'Daily walking gait analysis from iPhone motion sensors. Captures biomechanical walking parameters that can indicate fall risk, injury, or neurological changes.';

COMMENT ON COLUMN silver.daily_walking_gait.sk_date IS 'Surrogate date key in YYYYMMDD integer format.';
COMMENT ON COLUMN silver.daily_walking_gait.date IS 'Calendar date for this gait analysis.';
COMMENT ON COLUMN silver.daily_walking_gait.walking_speed_avg_km_hr IS 'Average walking speed. Unit: km/h. Typical range: 4.0-6.0 km/h. Declining walking speed is a strong predictor of frailty in older adults.';
COMMENT ON COLUMN silver.daily_walking_gait.walking_step_length_avg_cm IS 'Average step length. Unit: centimeters. Typical: 60-80 cm. Shorter steps can indicate balance issues or musculoskeletal problems.';
COMMENT ON COLUMN silver.daily_walking_gait.walking_asymmetry_avg_pct IS 'Average walking asymmetry as a percentage. Ideal: <5%. Values >10% may indicate injury, leg length discrepancy, or joint problems. Source: iPhone accelerometer.';
COMMENT ON COLUMN silver.daily_walking_gait.walking_double_support_avg_pct IS 'Average percentage of gait cycle spent with both feet on the ground. Normal: 20-30%. Higher values indicate more cautious gait, potentially due to balance concerns.';
COMMENT ON COLUMN silver.daily_walking_gait.walking_heart_rate_avg IS 'Average heart rate during walking bouts. Unit: bpm. Provides a standardized submaximal fitness indicator. Lower values at same speed indicate better cardiovascular fitness.';
COMMENT ON COLUMN silver.daily_walking_gait.walking_steadiness_pct IS 'Apple Walking Steadiness metric. Percentage score. OK: >80%. Low: 40-80%. Very Low: <40%. Predicts 12-month fall risk.';
COMMENT ON COLUMN silver.daily_walking_gait.business_key_hash IS 'MD5/SHA hash of the natural business key columns.';
COMMENT ON COLUMN silver.daily_walking_gait.row_hash IS 'MD5/SHA hash of all non-key columns for change detection.';
COMMENT ON COLUMN silver.daily_walking_gait.load_datetime IS 'Timestamp when this row was first loaded into the silver layer.';
COMMENT ON COLUMN silver.daily_walking_gait.update_datetime IS 'Timestamp when this row was last updated in the silver layer.';

-- =============================================================================
-- 13. silver.heart_rate
-- =============================================================================
COMMENT ON TABLE silver.heart_rate IS 'Continuous heart rate readings from wearable sensors. High-frequency time series (typically every 5 minutes or per-beat). Foundation for resting HR, HRV, and cardiovascular analysis.';

COMMENT ON COLUMN silver.heart_rate.sk_date IS 'Surrogate date key in YYYYMMDD integer format.';
COMMENT ON COLUMN silver.heart_rate.sk_time IS 'Surrogate time key as HH:MM string.';
COMMENT ON COLUMN silver.heart_rate.timestamp IS 'UTC timestamp of the heart rate measurement.';
COMMENT ON COLUMN silver.heart_rate.bpm IS 'Heart rate in beats per minute. Resting: 60-100 bpm (athletes: 40-60). Max HR ~220 minus age. Source: optical PPG sensor on wearable.';
COMMENT ON COLUMN silver.heart_rate.source_name IS 'Name of the device that produced the reading (e.g., Oura, Apple Watch).';
COMMENT ON COLUMN silver.heart_rate.business_key_hash IS 'MD5/SHA hash of the natural business key columns.';
COMMENT ON COLUMN silver.heart_rate.row_hash IS 'MD5/SHA hash of all non-key columns for change detection.';
COMMENT ON COLUMN silver.heart_rate.load_datetime IS 'Timestamp when this row was first loaded into the silver layer.';
COMMENT ON COLUMN silver.heart_rate.update_datetime IS 'Timestamp when this row was last updated in the silver layer.';

-- =============================================================================
-- 14. silver.mindful_session
-- =============================================================================
COMMENT ON TABLE silver.mindful_session IS 'Mindfulness and meditation sessions logged by apps (e.g., Headspace, Apple Health). Each row is one completed session. Used for correlating mindfulness practice with stress, sleep, and recovery.';

COMMENT ON COLUMN silver.mindful_session.sk_date IS 'Surrogate date key in YYYYMMDD integer format.';
COMMENT ON COLUMN silver.mindful_session.sk_time IS 'Surrogate time key as HH:MM string.';
COMMENT ON COLUMN silver.mindful_session.timestamp IS 'UTC timestamp when the mindfulness session started.';
COMMENT ON COLUMN silver.mindful_session.end_timestamp IS 'UTC timestamp when the mindfulness session ended.';
COMMENT ON COLUMN silver.mindful_session.duration_seconds IS 'Duration of the mindfulness session. Unit: seconds. Typical range: 300-1800 seconds (5-30 minutes). Research suggests 10+ minutes yields measurable HRV benefits.';
COMMENT ON COLUMN silver.mindful_session.source_name IS 'Name of the app or device that recorded the session (e.g., Headspace, Apple Watch Mindfulness).';
COMMENT ON COLUMN silver.mindful_session.business_key_hash IS 'MD5/SHA hash of the natural business key columns.';
COMMENT ON COLUMN silver.mindful_session.row_hash IS 'MD5/SHA hash of all non-key columns for change detection.';
COMMENT ON COLUMN silver.mindful_session.load_datetime IS 'Timestamp when this row was first loaded into the silver layer.';
COMMENT ON COLUMN silver.mindful_session.update_datetime IS 'Timestamp when this row was last updated in the silver layer.';

-- =============================================================================
-- 15. silver.personal_info
-- =============================================================================
COMMENT ON TABLE silver.personal_info IS 'Static personal demographics and body metrics. Single-row table containing age, sex, and body dimensions used for BMI, calorie, and heart rate zone calculations.';

COMMENT ON COLUMN silver.personal_info.age IS 'Age in years. Used for max HR estimation (220-age), BMR calculation, and age-adjusted reference ranges.';
COMMENT ON COLUMN silver.personal_info.weight_kg IS 'Body weight. Unit: kilograms. Used for BMI, calorie targets, and protein intake calculations.';
COMMENT ON COLUMN silver.personal_info.height_m IS 'Height. Unit: meters. Used for BMI calculation (weight_kg / height_m^2). Normal BMI: 18.5-24.9.';
COMMENT ON COLUMN silver.personal_info.biological_sex IS 'Biological sex (male/female). Affects reference ranges for resting HR, HRV, body composition, and calorie calculations.';
COMMENT ON COLUMN silver.personal_info.email IS 'Email address associated with the health data account. Used for identification, not analytics.';
COMMENT ON COLUMN silver.personal_info.business_key_hash IS 'MD5/SHA hash of the natural business key columns.';
COMMENT ON COLUMN silver.personal_info.row_hash IS 'MD5/SHA hash of all non-key columns for change detection.';
COMMENT ON COLUMN silver.personal_info.load_datetime IS 'Timestamp when this row was first loaded into the silver layer.';
COMMENT ON COLUMN silver.personal_info.update_datetime IS 'Timestamp when this row was last updated in the silver layer.';

-- =============================================================================
-- 16. silver.respiratory_rate
-- =============================================================================
COMMENT ON TABLE silver.respiratory_rate IS 'Respiratory rate measurements, primarily during sleep. Each row is a measurement period. Respiratory rate is a sensitive vital sign for illness detection and autonomic health.';

COMMENT ON COLUMN silver.respiratory_rate.sk_date IS 'Surrogate date key in YYYYMMDD integer format.';
COMMENT ON COLUMN silver.respiratory_rate.sk_time IS 'Surrogate time key as HH:MM string.';
COMMENT ON COLUMN silver.respiratory_rate.timestamp IS 'UTC timestamp when the respiratory rate measurement started.';
COMMENT ON COLUMN silver.respiratory_rate.end_timestamp IS 'UTC timestamp when the respiratory rate measurement ended.';
COMMENT ON COLUMN silver.respiratory_rate.breaths_per_min IS 'Respiratory rate. Unit: breaths per minute. Normal adult resting: 12-20 breaths/min. Elevated rates (>20) at rest may indicate stress, fever, or respiratory issues.';
COMMENT ON COLUMN silver.respiratory_rate.source_name IS 'Name of the device that measured respiratory rate (e.g., Oura Ring, Apple Watch).';
COMMENT ON COLUMN silver.respiratory_rate.business_key_hash IS 'MD5/SHA hash of the natural business key columns.';
COMMENT ON COLUMN silver.respiratory_rate.row_hash IS 'MD5/SHA hash of all non-key columns for change detection.';
COMMENT ON COLUMN silver.respiratory_rate.load_datetime IS 'Timestamp when this row was first loaded into the silver layer.';
COMMENT ON COLUMN silver.respiratory_rate.update_datetime IS 'Timestamp when this row was last updated in the silver layer.';

-- =============================================================================
-- 17. silver.step_count
-- =============================================================================
COMMENT ON TABLE silver.step_count IS 'Granular step count data from phone and wearable accelerometers. Each row covers a time interval (typically 1 hour or less). Used for intraday movement pattern analysis.';

COMMENT ON COLUMN silver.step_count.sk_date IS 'Surrogate date key in YYYYMMDD integer format.';
COMMENT ON COLUMN silver.step_count.sk_time IS 'Surrogate time key as HH:MM string.';
COMMENT ON COLUMN silver.step_count.timestamp IS 'UTC timestamp for the start of the step counting interval.';
COMMENT ON COLUMN silver.step_count.end_timestamp IS 'UTC timestamp for the end of the step counting interval.';
COMMENT ON COLUMN silver.step_count.duration_seconds IS 'Duration of the step counting interval. Unit: seconds.';
COMMENT ON COLUMN silver.step_count.step_count IS 'Number of steps recorded during the interval. Cadence (steps/min) can be derived: step_count / (duration_seconds / 60). Walking cadence ~100-130 steps/min, running ~150-190.';
COMMENT ON COLUMN silver.step_count.source_name IS 'Name of the device that counted steps (e.g., iPhone, Apple Watch, Oura).';
COMMENT ON COLUMN silver.step_count.business_key_hash IS 'MD5/SHA hash of the natural business key columns.';
COMMENT ON COLUMN silver.step_count.row_hash IS 'MD5/SHA hash of all non-key columns for change detection.';
COMMENT ON COLUMN silver.step_count.load_datetime IS 'Timestamp when this row was first loaded into the silver layer.';
COMMENT ON COLUMN silver.step_count.update_datetime IS 'Timestamp when this row was last updated in the silver layer.';

-- =============================================================================
-- 18. silver.toothbrushing
-- =============================================================================
COMMENT ON TABLE silver.toothbrushing IS 'Toothbrushing sessions detected by smart toothbrush or inferred from wearable motion. Used as a behavioral consistency proxy and circadian routine marker.';

COMMENT ON COLUMN silver.toothbrushing.sk_date IS 'Surrogate date key in YYYYMMDD integer format.';
COMMENT ON COLUMN silver.toothbrushing.sk_time IS 'Surrogate time key as HH:MM string.';
COMMENT ON COLUMN silver.toothbrushing.timestamp IS 'UTC timestamp when the toothbrushing session started.';
COMMENT ON COLUMN silver.toothbrushing.end_timestamp IS 'UTC timestamp when the toothbrushing session ended.';
COMMENT ON COLUMN silver.toothbrushing.duration_seconds IS 'Duration of the brushing session. Unit: seconds. Dentist recommendation: 120 seconds (2 minutes) minimum.';
COMMENT ON COLUMN silver.toothbrushing.source_name IS 'Name of the device or app that detected the brushing session.';
COMMENT ON COLUMN silver.toothbrushing.business_key_hash IS 'MD5/SHA hash of the natural business key columns.';
COMMENT ON COLUMN silver.toothbrushing.row_hash IS 'MD5/SHA hash of all non-key columns for change detection.';
COMMENT ON COLUMN silver.toothbrushing.load_datetime IS 'Timestamp when this row was first loaded into the silver layer.';
COMMENT ON COLUMN silver.toothbrushing.update_datetime IS 'Timestamp when this row was last updated in the silver layer.';

-- =============================================================================
-- 19. silver.water_intake
-- =============================================================================
COMMENT ON TABLE silver.water_intake IS 'Water and fluid intake logs from health tracking apps. Each row is a single intake entry. Hydration affects HRV, cognitive function, and exercise performance.';

COMMENT ON COLUMN silver.water_intake.sk_date IS 'Surrogate date key in YYYYMMDD integer format.';
COMMENT ON COLUMN silver.water_intake.sk_time IS 'Surrogate time key as HH:MM string.';
COMMENT ON COLUMN silver.water_intake.timestamp IS 'UTC timestamp when the water intake was logged.';
COMMENT ON COLUMN silver.water_intake.water_ml IS 'Volume of water consumed. Unit: milliliters. Daily target varies by body weight; general guideline: 30-40 ml per kg body weight. Source: manual logging app.';
COMMENT ON COLUMN silver.water_intake.source_name IS 'Name of the app used to log the water intake.';
COMMENT ON COLUMN silver.water_intake.business_key_hash IS 'MD5/SHA hash of the natural business key columns.';
COMMENT ON COLUMN silver.water_intake.row_hash IS 'MD5/SHA hash of all non-key columns for change detection.';
COMMENT ON COLUMN silver.water_intake.load_datetime IS 'Timestamp when this row was first loaded into the silver layer.';
COMMENT ON COLUMN silver.water_intake.update_datetime IS 'Timestamp when this row was last updated in the silver layer.';

-- =============================================================================
-- 20. silver.weight
-- =============================================================================
COMMENT ON TABLE silver.weight IS 'Body weight and composition measurements from a smart scale. Includes weight, fat mass, bone mass, muscle mass, and hydration. Key for tracking body composition trends.';

COMMENT ON COLUMN silver.weight.sk_date IS 'Surrogate date key in YYYYMMDD integer format.';
COMMENT ON COLUMN silver.weight.sk_time IS 'Surrogate time key as HH:MM string.';
COMMENT ON COLUMN silver.weight.datetime IS 'UTC timestamp of the weighing session.';
COMMENT ON COLUMN silver.weight.weight_kg IS 'Total body weight. Unit: kilograms. Best measured at the same time daily (morning, post-void) for consistent trending.';
COMMENT ON COLUMN silver.weight.fat_mass_kg IS 'Body fat mass estimated by bioelectrical impedance. Unit: kilograms. Healthy body fat: 10-20% for men, 18-28% for women. Smart scale estimates have ~3-5% error margin.';
COMMENT ON COLUMN silver.weight.bone_mass_kg IS 'Estimated bone mineral mass. Unit: kilograms. Typical adult: 2.5-4.0 kg. Relatively stable; significant changes may indicate metabolic bone disease.';
COMMENT ON COLUMN silver.weight.muscle_mass_kg IS 'Estimated skeletal muscle mass. Unit: kilograms. Important for metabolic health and functional capacity. Progressive decline with age (sarcopenia) is a key health risk.';
COMMENT ON COLUMN silver.weight.hydration_kg IS 'Estimated body water mass. Unit: kilograms. Healthy range: 50-65% of body weight. Low values may indicate dehydration. Affected by time of day and recent fluid intake.';
COMMENT ON COLUMN silver.weight.business_key_hash IS 'MD5/SHA hash of the natural business key columns.';
COMMENT ON COLUMN silver.weight.row_hash IS 'MD5/SHA hash of all non-key columns for change detection.';
COMMENT ON COLUMN silver.weight.load_datetime IS 'Timestamp when this row was first loaded into the silver layer.';
COMMENT ON COLUMN silver.weight.update_datetime IS 'Timestamp when this row was last updated in the silver layer.';

-- =============================================================================
-- 21. silver.workout
-- =============================================================================
COMMENT ON TABLE silver.workout IS 'Individual workout sessions from fitness trackers and apps. Each row is one workout with activity type, duration, intensity, and calorie burn. Core input for training load analysis.';

COMMENT ON COLUMN silver.workout.sk_date IS 'Surrogate date key in YYYYMMDD integer format.';
COMMENT ON COLUMN silver.workout.day IS 'Calendar date of the workout.';
COMMENT ON COLUMN silver.workout.workout_id IS 'Unique identifier for the workout session from the source system.';
COMMENT ON COLUMN silver.workout.activity IS 'Type of activity performed (e.g., running, cycling, strength_training, swimming, yoga).';
COMMENT ON COLUMN silver.workout.intensity IS 'Subjective or calculated intensity level (e.g., easy, moderate, hard). Source-dependent classification.';
COMMENT ON COLUMN silver.workout.calories IS 'Estimated calories burned during the workout. Unit: kilocalories. Accuracy varies by activity type and device.';
COMMENT ON COLUMN silver.workout.distance_meters IS 'Distance covered during the workout. Unit: meters. Applicable to running, cycling, swimming, walking. NULL for stationary activities.';
COMMENT ON COLUMN silver.workout.start_datetime IS 'UTC timestamp when the workout started.';
COMMENT ON COLUMN silver.workout.end_datetime IS 'UTC timestamp when the workout ended.';
COMMENT ON COLUMN silver.workout.duration_seconds IS 'Total workout duration. Unit: seconds. Includes warm-up and cool-down if tracked.';
COMMENT ON COLUMN silver.workout.label IS 'User-assigned label or name for the workout (e.g., "Morning run", "Leg day").';
COMMENT ON COLUMN silver.workout.source IS 'Source device or app that recorded the workout (e.g., Oura, Apple Watch, Strava).';
COMMENT ON COLUMN silver.workout.business_key_hash IS 'MD5/SHA hash of the natural business key columns.';
COMMENT ON COLUMN silver.workout.row_hash IS 'MD5/SHA hash of all non-key columns for change detection.';
COMMENT ON COLUMN silver.workout.load_datetime IS 'Timestamp when this row was first loaded into the silver layer.';
COMMENT ON COLUMN silver.workout.update_datetime IS 'Timestamp when this row was last updated in the silver layer.';

-- =============================================================================
-- silver.metric_relationships
-- =============================================================================
COMMENT ON TABLE silver.metric_relationships IS 'Quantified statistical relationships between health metrics. Populated by correlation analysis pipelines. Used by the agent to understand cross-metric dependencies and generate hypotheses.';

COMMENT ON COLUMN silver.metric_relationships.source_metric IS 'Name of the independent/predictor metric in the relationship (e.g., sleep_score, steps, hrv).';
COMMENT ON COLUMN silver.metric_relationships.target_metric IS 'Name of the dependent/outcome metric in the relationship (e.g., readiness_score, resting_hr).';
COMMENT ON COLUMN silver.metric_relationships.relationship_type IS 'Type of statistical relationship: correlation, granger_causality, mutual_information, or domain_knowledge.';
COMMENT ON COLUMN silver.metric_relationships.strength IS 'Strength of the relationship. For correlations: -1.0 to 1.0. For mutual information: 0.0+. For domain knowledge: subjective 0.0-1.0.';
COMMENT ON COLUMN silver.metric_relationships.lag_days IS 'Time lag in days between source and target metric. 0 = same day. Positive = source leads target. E.g., lag_days=1 means yesterday source predicts today target.';
COMMENT ON COLUMN silver.metric_relationships.direction IS 'Direction of effect: positive (source up -> target up), negative (source up -> target down), or bidirectional.';
COMMENT ON COLUMN silver.metric_relationships.evidence_type IS 'How the relationship was established: computed (statistical analysis), literature (medical research), or observed (user-specific pattern).';
COMMENT ON COLUMN silver.metric_relationships.confidence IS 'Confidence in the relationship strength. 0.0-1.0. Based on sample size, p-value, or expert assessment.';
COMMENT ON COLUMN silver.metric_relationships.description IS 'Human-readable description of the relationship and its practical implications.';
COMMENT ON COLUMN silver.metric_relationships.last_computed_at IS 'Timestamp when this relationship was last recalculated or validated.';

-- =============================================================================
-- agent.patient_profile
-- =============================================================================
COMMENT ON TABLE agent.patient_profile IS 'Core memory store for patient profile facts. Always loaded into agent context. Key-value pairs covering demographics, baselines, goals, and computed health markers.';

COMMENT ON COLUMN agent.patient_profile.profile_key IS 'Unique identifier for the profile fact (e.g., age, resting_hr_baseline, sleep_goal_hours).';
COMMENT ON COLUMN agent.patient_profile.profile_value IS 'Human-readable string value of the profile fact.';
COMMENT ON COLUMN agent.patient_profile.numeric_value IS 'Numeric representation of the value, when applicable. NULL for text-only facts. Enables range comparisons and computations.';
COMMENT ON COLUMN agent.patient_profile.category IS 'Grouping category: demographics, baseline, goal, preference, computed, or medical.';
COMMENT ON COLUMN agent.patient_profile.description IS 'Human-readable explanation of what this profile fact represents.';
COMMENT ON COLUMN agent.patient_profile.computed_from IS 'SQL query or description of how this value was derived, if it is a computed field. NULL for manually entered facts.';
COMMENT ON COLUMN agent.patient_profile.last_updated_at IS 'Timestamp when this profile fact was last updated.';
COMMENT ON COLUMN agent.patient_profile.update_frequency IS 'How often this value should be refreshed: daily, weekly, monthly, on_change, or static.';

-- =============================================================================
-- agent.daily_summaries
-- =============================================================================
COMMENT ON TABLE agent.daily_summaries IS 'Recall memory: one row per day with key health metrics and a natural language summary. Embeddings enable semantic search over historical health context.';

COMMENT ON COLUMN agent.daily_summaries.day IS 'Calendar date for this daily summary.';
COMMENT ON COLUMN agent.daily_summaries.sleep_score IS 'Oura sleep score for this day (0-100).';
COMMENT ON COLUMN agent.daily_summaries.readiness_score IS 'Oura readiness score for this day (0-100).';
COMMENT ON COLUMN agent.daily_summaries.steps IS 'Total step count for the day.';
COMMENT ON COLUMN agent.daily_summaries.resting_hr IS 'Resting heart rate for the night. Unit: bpm.';
COMMENT ON COLUMN agent.daily_summaries.stress_level IS 'Qualitative stress level: restored, normal, stressed, or high_stress.';
COMMENT ON COLUMN agent.daily_summaries.has_anomaly IS 'Whether any metric on this day was flagged as anomalous (outside 2+ standard deviations from personal baseline).';
COMMENT ON COLUMN agent.daily_summaries.anomaly_metrics IS 'Comma-separated list of metric names that were anomalous on this day. NULL if has_anomaly is FALSE.';
COMMENT ON COLUMN agent.daily_summaries.summary_text IS 'Natural language summary of the day health profile, suitable for direct inclusion in agent context.';
COMMENT ON COLUMN agent.daily_summaries.embedding IS 'FLOAT[384] vector embedding of summary_text. Generated by a sentence transformer model for semantic similarity search.';
COMMENT ON COLUMN agent.daily_summaries.data_completeness IS 'Fraction of expected data sources that reported data on this day. 0.0-1.0. Below 0.5 means significant data gaps.';
COMMENT ON COLUMN agent.daily_summaries.created_at IS 'Timestamp when this daily summary was generated.';

-- =============================================================================
-- agent.health_graph
-- =============================================================================
COMMENT ON TABLE agent.health_graph IS 'Relationship memory: nodes in the health knowledge graph. Encodes biomarkers, conditions, supplements, activities, and concepts as a semantic network for agent reasoning.';

COMMENT ON COLUMN agent.health_graph.node_id IS 'Unique node identifier using type:name convention (e.g., biomarker:sleep_score, supplement:magnesium, condition:poor_sleep).';
COMMENT ON COLUMN agent.health_graph.node_type IS 'Category of the node: biomarker, supplement, condition, concept, activity, nutrient, or organ_system.';
COMMENT ON COLUMN agent.health_graph.node_label IS 'Human-readable display name for the node.';
COMMENT ON COLUMN agent.health_graph.description IS 'Detailed description of the node, including clinical significance and normal ranges where applicable.';
COMMENT ON COLUMN agent.health_graph.related_tables IS 'Comma-separated list of silver-layer tables containing data for this node (e.g., daily_sleep, daily_readiness).';
COMMENT ON COLUMN agent.health_graph.related_columns IS 'Comma-separated list of specific columns in the related tables (e.g., sleep_score, readiness_score).';

-- =============================================================================
-- agent.health_graph_edges
-- =============================================================================
COMMENT ON TABLE agent.health_graph_edges IS 'Relationship memory: edges in the health knowledge graph. Encodes directed relationships between health concepts with type, strength, and evidence.';

COMMENT ON COLUMN agent.health_graph_edges.source_node_id IS 'Node ID of the relationship source (the cause, predictor, or subject).';
COMMENT ON COLUMN agent.health_graph_edges.target_node_id IS 'Node ID of the relationship target (the effect, outcome, or object).';
COMMENT ON COLUMN agent.health_graph_edges.edge_type IS 'Relationship type: improves, worsens, measured_by, part_of, indicates, correlates_with, prevents, treats, requires, inhibits, or modulates.';
COMMENT ON COLUMN agent.health_graph_edges.weight IS 'Strength/importance of the relationship. 0.0-1.0 scale. Higher = stronger effect or more reliable evidence.';
COMMENT ON COLUMN agent.health_graph_edges.evidence IS 'Source of evidence: clinical_trial, meta_analysis, observational_study, mechanistic, expert_consensus, or user_observed.';
COMMENT ON COLUMN agent.health_graph_edges.description IS 'Human-readable explanation of the relationship and its practical health implications.';

-- =============================================================================
-- agent.knowledge_base
-- =============================================================================
COMMENT ON TABLE agent.knowledge_base IS 'Archival memory: long-term insight store. The agent writes derived insights, discovered patterns, and recommendations here. Vector-searchable via embeddings for retrieval-augmented reasoning.';

COMMENT ON COLUMN agent.knowledge_base.insight_id IS 'Unique identifier for the insight (UUID or structured ID).';
COMMENT ON COLUMN agent.knowledge_base.created_at IS 'Timestamp when the insight was created.';
COMMENT ON COLUMN agent.knowledge_base.insight_type IS 'Category of insight: pattern, anomaly, recommendation, correlation, trend, or hypothesis.';
COMMENT ON COLUMN agent.knowledge_base.title IS 'Short descriptive title for the insight.';
COMMENT ON COLUMN agent.knowledge_base.content IS 'Full text content of the insight, including evidence, reasoning, and actionable recommendations.';
COMMENT ON COLUMN agent.knowledge_base.evidence_query IS 'SQL query that can reproduce the evidence supporting this insight. Enables verification and refresh.';
COMMENT ON COLUMN agent.knowledge_base.confidence IS 'Agent confidence in this insight. 0.0-1.0. Based on data quality, sample size, and consistency of evidence.';
COMMENT ON COLUMN agent.knowledge_base.tags IS 'Array of tags for categorization and filtering (e.g., [sleep, hrv, recovery]).';
COMMENT ON COLUMN agent.knowledge_base.embedding IS 'FLOAT[384] vector embedding of title + content. Generated by a sentence transformer for semantic retrieval.';
COMMENT ON COLUMN agent.knowledge_base.is_active IS 'Whether this insight is still considered valid. FALSE if superseded or invalidated by newer evidence.';
COMMENT ON COLUMN agent.knowledge_base.superseded_by IS 'insight_id of the newer insight that replaces this one. NULL if still active.';

-- =============================================================================
-- silver.dim_marker_catalog
-- =============================================================================
COMMENT ON TABLE silver.dim_marker_catalog IS 'Canonical biomarker reference catalog. One row per marker. Defines the display name, domain, body system, canonical unit, and synonyms for cross-lab normalization. Used by MCP tools to resolve marker names and units consistently across lab_results data.';

COMMENT ON COLUMN silver.dim_marker_catalog.marker_key IS 'Primary key. Stable snake_case identifier for the marker (e.g., hdl_cholesterol, tsh). Used as foreign key in dim_reference_range and for joining to lab_results.marker_name after normalization.';
COMMENT ON COLUMN silver.dim_marker_catalog.display_name IS 'Human-readable marker name for UI display and natural-language responses (e.g., "HDL Cholesterol", "Thyroid Stimulating Hormone").';
COMMENT ON COLUMN silver.dim_marker_catalog.marker_domain IS 'Broad analytical domain: lipids, thyroid, hematology, liver, kidney, vitamins, hormones, inflammation, metabolic, microbiome, etc. Use for filtering markers by clinical area.';
COMMENT ON COLUMN silver.dim_marker_catalog.body_system IS 'Physiological system the marker relates to: cardiovascular, endocrine, hepatic, renal, immune, musculoskeletal, gastrointestinal, neurological, etc. Enables body-system-level health summaries.';
COMMENT ON COLUMN silver.dim_marker_catalog.canonical_unit IS 'Standard unit of measurement for this marker (e.g., mmol/L, µg/L, %). NULL for dimensionless markers (pH, qualitative results). Lab results should be converted to this unit before comparison.';
COMMENT ON COLUMN silver.dim_marker_catalog.description IS 'Plain-language explanation of what the marker measures, why it matters clinically, and how it relates to health outcomes. Written for AI agent context injection.';
COMMENT ON COLUMN silver.dim_marker_catalog.data_type IS 'Expected data type of the marker value: numeric (default), categorical, boolean, or text. Determines how to interpret value_numeric vs value_text in lab_results.';
COMMENT ON COLUMN silver.dim_marker_catalog.is_log_scale IS 'Whether the marker is best interpreted on a logarithmic scale (e.g., antibody titers, bacterial counts). TRUE means percentage changes are more meaningful than absolute differences.';
COMMENT ON COLUMN silver.dim_marker_catalog.detection_limit IS 'Lower detection limit of the assay. Values reported as "< X" in lab results should be stored as this value. NULL if not applicable.';
COMMENT ON COLUMN silver.dim_marker_catalog.synonyms IS 'Comma-separated list of alternative names and abbreviations used by different labs (e.g., "HDL-C, HDL cholesterol, high-density lipoprotein"). Used for fuzzy matching during lab result ingestion.';
COMMENT ON COLUMN silver.dim_marker_catalog.load_datetime IS 'Timestamp when this catalog entry was first loaded into the silver layer.';

-- =============================================================================
-- silver.dim_reference_range
-- =============================================================================
COMMENT ON TABLE silver.dim_reference_range IS 'Cross-lab reference ranges for biomarkers. Multiple rows per marker: one per lab source, age group, and sex stratum. Supports temporal validity via effective_from/effective_to. Join to dim_marker_catalog on marker_key for marker metadata.';

COMMENT ON COLUMN silver.dim_reference_range.marker_key IS 'Foreign key to dim_marker_catalog.marker_key. Identifies which biomarker this reference range applies to.';
COMMENT ON COLUMN silver.dim_reference_range.source IS 'Origin of the reference range: lab name (e.g., "Rigshospitalet"), guideline body (e.g., "ESC 2021"), or "optimal" for evidence-based optimal ranges. Part of the composite primary key.';
COMMENT ON COLUMN silver.dim_reference_range.reference_type IS 'Classification of the range: standard (lab default), optimal (evidence-based target), clinical (guideline-defined threshold), or custom (user-defined goal). Use to select which range to apply for status evaluation.';
COMMENT ON COLUMN silver.dim_reference_range.reference_min IS 'Lower bound of the reference range in the unit specified by the unit column. NULL if the range is one-sided (e.g., "< 5.0 mmol/L" for LDL has no lower bound).';
COMMENT ON COLUMN silver.dim_reference_range.reference_max IS 'Upper bound of the reference range in the unit specified by the unit column. NULL if the range is one-sided (e.g., "> 1.0 mmol/L" for HDL has no upper bound).';
COMMENT ON COLUMN silver.dim_reference_range.unit IS 'Unit of measurement for reference_min and reference_max. Should match dim_marker_catalog.canonical_unit. NULL for dimensionless markers.';
COMMENT ON COLUMN silver.dim_reference_range.age_group IS 'Age stratum this range applies to. Default: "adult". Other values: "pediatric", "elderly", "18-39", "40-59", "60+". Part of the composite primary key.';
COMMENT ON COLUMN silver.dim_reference_range.sex IS 'Sex stratum this range applies to. Default: "all". Other values: "male", "female". Part of the composite primary key. Some markers (e.g., testosterone, hemoglobin) have sex-specific ranges.';
COMMENT ON COLUMN silver.dim_reference_range.notes IS 'Additional context about the reference range: fasting requirements, assay method, clinical caveats, or source publication. Useful for AI agent explanations.';
COMMENT ON COLUMN silver.dim_reference_range.effective_from IS 'Date from which this reference range is valid. NULL means valid since the beginning of data collection. Enables tracking when labs update their ranges.';
COMMENT ON COLUMN silver.dim_reference_range.effective_to IS 'Date until which this reference range is valid. NULL means currently active. When a lab updates ranges, set effective_to on the old row and insert a new row.';
COMMENT ON COLUMN silver.dim_reference_range.load_datetime IS 'Timestamp when this reference range row was first loaded into the silver layer.';

-- =============================================================================
-- silver.genetic_ancestry
-- =============================================================================
COMMENT ON TABLE silver.genetic_ancestry IS 'Ancestry composition summary from genotyping platforms (e.g., 23andMe). One row per population group. Contains population-level percentage breakdowns of genetic ancestry. Static data — loaded once per genotyping report.';

COMMENT ON COLUMN silver.genetic_ancestry.source IS 'Genotyping platform that produced the ancestry report (e.g., "23andme", "ancestry_dna"). Default: "23andme".';
COMMENT ON COLUMN silver.genetic_ancestry.population IS 'Specific population or ethnic group identified in the ancestry analysis (e.g., "Finnish", "British & Irish", "Broadly Northwestern European"). Granularity depends on the platform.';
COMMENT ON COLUMN silver.genetic_ancestry.region IS 'Geographic region grouping the population belongs to (e.g., "Northwestern European", "East Asian", "Sub-Saharan African"). Use for higher-level ancestry summaries.';
COMMENT ON COLUMN silver.genetic_ancestry.percentage IS 'Estimated percentage of total ancestry attributed to this population. Values sum to approximately 100% across all rows for a given source and confidence_level. Range: 0.0-100.0.';
COMMENT ON COLUMN silver.genetic_ancestry.confidence_level IS 'Statistical confidence threshold used for the estimate: "speculative" (50%), "standard" (75%), or "conservative" (90%). Higher confidence = fewer and broader population categories. Default: "standard".';
COMMENT ON COLUMN silver.genetic_ancestry.report_date IS 'Date the ancestry report was generated by the platform. NULL if not available from the source data.';
COMMENT ON COLUMN silver.genetic_ancestry.business_key_hash IS 'MD5/SHA hash of the natural business key columns (source, population, confidence_level). Used for SCD tracking and deduplication.';
COMMENT ON COLUMN silver.genetic_ancestry.row_hash IS 'MD5/SHA hash of all non-key columns. Used to detect changes in ancestry estimates across report updates.';
COMMENT ON COLUMN silver.genetic_ancestry.load_datetime IS 'Timestamp when this row was first loaded into the silver layer.';
COMMENT ON COLUMN silver.genetic_ancestry.update_datetime IS 'Timestamp when this row was last updated in the silver layer.';

-- =============================================================================
-- silver.genetic_traits
-- =============================================================================
COMMENT ON TABLE silver.genetic_traits IS 'Wellness traits, haplogroups, and neanderthal ancestry from genotyping platforms (e.g., 23andMe). One row per trait or finding. Covers categories like wellness, carrier_status, haplogroup, and neanderthal. Static data — loaded once per genotyping report.';

COMMENT ON COLUMN silver.genetic_traits.source IS 'Genotyping platform that produced the trait report (e.g., "23andme"). Default: "23andme".';
COMMENT ON COLUMN silver.genetic_traits.category IS 'Trait category grouping: wellness (e.g., caffeine metabolism), carrier_status (e.g., cystic fibrosis), haplogroup (maternal/paternal lineage), neanderthal (archaic ancestry), or pharmacogenomics.';
COMMENT ON COLUMN silver.genetic_traits.trait_name IS 'Specific trait or report name (e.g., "Caffeine Consumption", "Maternal Haplogroup", "Neanderthal Ancestry"). Unique within a source + category combination.';
COMMENT ON COLUMN silver.genetic_traits.result_value IS 'Primary result as text. Format varies by category: descriptive text for wellness ("Likely consumes more"), haplogroup designation ("H1a"), percentage for neanderthal ("2.1%"), carrier status ("Not a carrier").';
COMMENT ON COLUMN silver.genetic_traits.result_numeric IS 'Numeric result when applicable (e.g., neanderthal percentage as 2.1, number of neanderthal variants as 285). NULL for non-numeric results like haplogroup designations.';
COMMENT ON COLUMN silver.genetic_traits.gene IS 'Gene associated with the trait (e.g., "CYP1A2" for caffeine metabolism). NULL if the trait is polygenic or gene is not reported.';
COMMENT ON COLUMN silver.genetic_traits.snp_id IS 'dbSNP identifier for the variant (e.g., "rs762551"). NULL if the trait involves multiple SNPs or the specific variant is not reported.';
COMMENT ON COLUMN silver.genetic_traits.genotype IS 'Observed genotype at the SNP (e.g., "A/C", "G/G"). NULL if not reported or if the trait is based on a polygenic score.';
COMMENT ON COLUMN silver.genetic_traits.clinical_relevance IS 'Clinical significance level: informational (default for wellness/neanderthal), actionable (pharmacogenomics, carrier status with reproductive implications), or monitoring (relevant to ongoing health tracking).';
COMMENT ON COLUMN silver.genetic_traits.related_metrics IS 'Comma-separated list of silver-layer metric columns that this genetic trait influences (e.g., "deep_sleep_duration,caffeine_intake"). Used by the AI agent to contextualize wearable data with genetic predispositions.';
COMMENT ON COLUMN silver.genetic_traits.report_date IS 'Date the trait report was generated by the platform. NULL if not available from the source data.';
COMMENT ON COLUMN silver.genetic_traits.business_key_hash IS 'MD5/SHA hash of the natural business key columns (source, category, trait_name). Used for SCD tracking and deduplication.';
COMMENT ON COLUMN silver.genetic_traits.row_hash IS 'MD5/SHA hash of all non-key columns. Used to detect changes in trait data across report updates.';
COMMENT ON COLUMN silver.genetic_traits.load_datetime IS 'Timestamp when this row was first loaded into the silver layer.';
COMMENT ON COLUMN silver.genetic_traits.update_datetime IS 'Timestamp when this row was last updated in the silver layer.';
