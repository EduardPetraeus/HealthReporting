# Metric Dictionary — HealthReporting

> Comprehensive reference for all health metrics in the system.
> Last updated: 2026-03-08

---

## Overview

The platform tracks 24+ health metrics across 8 categories, sourced from 7 data providers. Each metric has a YAML semantic contract in `health_platform/contracts/metrics/` that defines computations, thresholds, baselines, and related metrics.

**Available computations** (vary by metric):
- `daily_value` — raw values for a day or date range
- `period_average` — aggregated statistics over a period
- `trend` — time series with rolling averages
- `anomaly` — z-score based anomaly detection (via `_business_rules.yml`)

---

## Quick Reference

| # | Metric | Category | Source(s) | Silver Table | Column | Unit | Grain |
|---|--------|----------|-----------|-------------|--------|------|-------|
| 1 | sleep_score | Sleep | Oura | daily_sleep | sleep_score | score (0-100) | daily |
| 2 | readiness_score | Recovery | Oura | daily_readiness | readiness_score | score (0-100) | daily |
| 3 | activity_score | Activity | Oura | daily_activity | activity_score | score (0-100) | daily |
| 4 | steps | Activity | Oura | daily_activity | steps | steps | daily |
| 5 | active_calories | Activity | Oura | daily_activity | active_calories | kcal | daily |
| 6 | resting_heart_rate | Vitals | Oura + Apple Health | heart_rate | bpm | bpm | per-measurement |
| 7 | weight | Body Composition | Withings | weight | weight_kg | kg | per-measurement |
| 8 | body_fat | Body Composition | Withings | weight | fat_mass_kg | kg | per-measurement |
| 9 | blood_pressure | Vitals | Withings + Apple Health | blood_pressure | systolic, diastolic | mmHg | per-measurement |
| 10 | body_temperature | Vitals | Apple Health + Oura | body_temperature | temperature_degc | degC | per-measurement |
| 11 | respiratory_rate | Vitals | Apple Health | respiratory_rate | breaths_per_min | breaths/min | per-measurement |
| 12 | spo2 | Vitals | Oura | daily_spo2 | spo2_avg_pct | % | daily |
| 13 | stress | Recovery | Oura | daily_stress | stress_high | seconds | daily |
| 14 | recovery | Recovery | Oura | daily_stress | recovery_high | seconds | daily |
| 15 | water_intake | Nutrition | Apple Health | water_intake | water_ml | ml | per-entry |
| 16 | calories | Nutrition | Lifesum | daily_meal | calories | kcal | per-meal |
| 17 | protein | Nutrition | Lifesum | daily_meal | protein | grams | per-meal |
| 18 | workout | Activity | Oura + Strava | workout | activity, duration_seconds, calories | multiple | per-workout |
| 19 | mindful_session | Mindfulness | Apple Health | mindful_session | duration_seconds | seconds | per-session |
| 20 | walking_gait | Gait | Apple Health | daily_walking_gait | walking_speed_kmh, step_length_cm, etc. | multiple | daily |
| 21 | toothbrushing | Hygiene | Apple Health | toothbrushing | duration_seconds | seconds | per-session |
| 22 | hrv | Vitals | Oura + Apple Health | heart_rate | hrv_ms | ms | per-measurement |
| 23 | weather | Environment | Open-Meteo | daily_weather | temp_max_c, precipitation_mm, etc. | various | daily |
| 24 | lab_biomarkers | Lab Results | Manual (YAML) | lab_results | value_numeric | varies | per-test |

---

## Detailed Metric Entries

### Sleep

#### 1. Sleep Score

| Field | Value |
|-------|-------|
| **Name** | `sleep_score` |
| **Display Name** | Sleep Score |
| **Source** | Oura Ring (API v2) |
| **Silver Table** | `silver.daily_sleep` |
| **Column** | `sleep_score` |
| **Unit** | score (0-100) |
| **Grain** | daily |
| **Contract** | `contracts/metrics/sleep_score.yml` |

**Description:** Oura Ring composite sleep quality score derived from total sleep duration, sleep efficiency, restfulness, REM sleep, deep sleep, latency, and timing. Higher scores indicate better overall sleep quality. Strongly predictive of next-day readiness and cognitive performance.

**Thresholds:**

| Range | Label |
|-------|-------|
| >= 85 | Excellent |
| 70-84 | Good |
| 55-69 | Fair |
| <= 54 | Needs attention |

**Computations:** daily_value, period_average, trend, distribution

**Related Metrics:**
- readiness_score (lag: 1 day) — strongly predicts next-day readiness
- resting_heart_rate (lag: 0) — poor sleep correlates with elevated resting HR
- daily_stress (lag: 1 day) — low sleep scores associate with higher stress next day

---

### Recovery

#### 2. Readiness Score

| Field | Value |
|-------|-------|
| **Name** | `readiness_score` |
| **Display Name** | Readiness Score |
| **Source** | Oura Ring (API v2) |
| **Silver Table** | `silver.daily_readiness` |
| **Column** | `readiness_score` |
| **Unit** | score (0-100) |
| **Grain** | daily |
| **Contract** | `contracts/metrics/readiness_score.yml` |

**Description:** Measures how prepared the body is for physical and mental exertion. Combines resting heart rate, HRV, body temperature deviation, sleep balance, previous day activity, and recovery index. A score below 70 suggests reducing training intensity and prioritizing recovery.

**Thresholds:**

| Range | Label |
|-------|-------|
| >= 85 | Excellent - peak performance day |
| 70-84 | Good - normal training OK |
| 55-69 | Fair - moderate activity only |
| <= 54 | Needs attention - prioritize recovery |

**Computations:** daily_value, period_average, trend, contributors

**Related Metrics:**
- sleep_score (lag: 0) — previous night sleep quality directly feeds readiness
- activity_score (lag: 1 day) — high activity lowers next-day readiness
- resting_heart_rate (lag: 0) — elevated resting HR contributes to low readiness
- daily_stress (lag: 0) — chronic stress reduces readiness

---

#### 13. Daily Stress

| Field | Value |
|-------|-------|
| **Name** | `daily_stress` |
| **Display Name** | Daily Stress |
| **Source** | Oura Ring (API v2) |
| **Silver Table** | `silver.daily_stress` |
| **Columns** | `stress_high`, `recovery_high`, `day_summary` |
| **Unit** | seconds / category |
| **Grain** | daily |
| **Contract** | `contracts/metrics/daily_stress.yml` |

**Description:** Derived from HRV, heart rate, skin temperature, and activity patterns. Categorized into states: restored, rest, normal, high_stress, and stressed. The `stress_high` and `recovery_high` fields indicate total seconds spent in high-stress and high-recovery states.

**Thresholds:**

| Category | Label |
|----------|-------|
| restored | Excellent recovery balance |
| rest | Low stress, adequate recovery |
| normal | Balanced stress and recovery |
| high_stress | Recovery needed |
| stressed | Prioritize rest and recovery |

**Computations:** daily_value, period_average, trend, stress_balance, distribution

**Related Metrics:**
- readiness_score (lag: 0) — chronic stress reduces readiness
- sleep_score (lag: 0) — high stress disrupts sleep quality
- resting_heart_rate (lag: 0) — stress elevates resting heart rate
- blood_pressure (lag: 0) — acute stress temporarily raises blood pressure

---

#### 14. Recovery

| Field | Value |
|-------|-------|
| **Name** | `recovery` |
| **Display Name** | Daily Recovery |
| **Source** | Oura Ring (API v2) |
| **Silver Table** | `silver.daily_stress` |
| **Column** | `recovery_high` |
| **Unit** | seconds |
| **Grain** | daily |

**Description:** Total seconds spent in high-recovery state. Tracked alongside stress as a complementary metric in `daily_stress`. A high recovery-to-stress ratio indicates good autonomic balance.

---

### Activity

#### 3. Activity Score

| Field | Value |
|-------|-------|
| **Name** | `activity_score` |
| **Display Name** | Activity Score |
| **Source** | Oura Ring (API v2) |
| **Silver Table** | `silver.daily_activity` |
| **Column** | `activity_score` |
| **Unit** | score (0-100) |
| **Grain** | daily |
| **Contract** | `contracts/metrics/activity_score.yml` |

**Description:** Measures overall movement and exercise quality. Factors include steps, active calories, training frequency, training volume, and inactivity alerts. Balances sufficient movement with avoiding overtraining.

**Thresholds:**

| Range | Label |
|-------|-------|
| >= 85 | Excellent - highly active day |
| 70-84 | Good - meeting activity goals |
| 55-69 | Fair - below target activity |
| <= 54 | Needs attention - too sedentary |

**Computations:** daily_value, period_average, trend, with_details

**Related Metrics:**
- readiness_score (lag: 1 day) — high activity today may reduce tomorrow's readiness
- steps (lag: 0) — primary driver of activity score
- calories (lag: 0) — active calories burned contribute to activity score
- workout (lag: 0) — structured workouts boost activity score

---

#### 4. Daily Steps

| Field | Value |
|-------|-------|
| **Name** | `steps` |
| **Display Name** | Daily Steps |
| **Source** | Oura Ring |
| **Silver Table** | `silver.daily_activity` |
| **Column** | `steps` |
| **Unit** | steps |
| **Grain** | daily |
| **Contract** | `contracts/metrics/steps.yml` |

**Description:** Total daily step count. WHO recommends 7000-8000 steps per day for health benefits, with diminishing returns above 10000. Consistently low step counts correlate with increased cardiovascular risk.

**Thresholds:**

| Range | Label |
|-------|-------|
| >= 10000 | Excellent - very active |
| 7000-9999 | Good - meeting WHO recommendation |
| 5000-6999 | Fair - somewhat active |
| <= 4999 | Needs attention - sedentary |

**Computations:** daily_value, period_average, trend, weekly_totals

**Related Metrics:**
- activity_score (lag: 0) — primary component of activity score
- calories (lag: 0) — more steps burn more calories through NEAT
- walking_gait (lag: 0) — gait quality affects step efficiency
- weight (lag: 30 days) — sustained step increases support weight management

---

#### 5. Active Calories

| Field | Value |
|-------|-------|
| **Name** | `active_calories` |
| **Display Name** | Active Calories |
| **Source** | Oura Ring |
| **Silver Table** | `silver.daily_activity` |
| **Column** | `active_calories` |
| **Unit** | kcal |
| **Grain** | daily |

**Description:** Calories burned through physical activity (excludes basal metabolic rate). Tracked alongside steps and activity score in `daily_activity`. A key component of daily energy expenditure.

---

#### 18. Workout

| Field | Value |
|-------|-------|
| **Name** | `workout` |
| **Display Name** | Workout |
| **Source** | Oura Ring + Strava |
| **Silver Table** | `silver.workout` |
| **Columns** | `activity`, `duration_seconds`, `calories`, `distance_meters`, `intensity` |
| **Unit** | multiple (duration in seconds, calories in kcal, distance in meters) |
| **Grain** | per-workout |
| **Contract** | `contracts/metrics/workout.yml` |

**Description:** Structured workout sessions with activity type, duration, calories burned, distance, average heart rate, and intensity. Multiple workouts can occur per day. Includes `source_system` column for cross-source deduplication (Oura, Strava, Lifesum).

**Thresholds:**

| Scope | Range | Label |
|-------|-------|-------|
| Weekly frequency | >= 4 | Excellent - consistent training |
| Weekly frequency | 3 | Good - adequate training stimulus |
| Weekly frequency | <= 2 | Low - consider increasing frequency |
| Session duration | >= 60 min | Long session |
| Session duration | 30-59 min | Moderate session |
| Session duration | < 30 min | Short session |

**Computations:** daily_value, period_summary, trend, by_activity, frequency

**Related Metrics:**
- activity_score (lag: 0) — workouts significantly boost daily activity score
- readiness_score (lag: 1 day) — intense workouts reduce next-day readiness
- resting_heart_rate (lag: 1 day) — acute post-exercise RHR elevation for 24-48h
- calories (lag: 0) — caloric intake should match training load
- sleep_score (lag: 0) — evening exercise may affect sleep quality

---

### Vitals

#### 6. Resting Heart Rate

| Field | Value |
|-------|-------|
| **Name** | `resting_heart_rate` |
| **Display Name** | Resting Heart Rate |
| **Source** | Oura Ring + Apple Health |
| **Silver Table** | `silver.heart_rate` |
| **Column** | `bpm` |
| **Unit** | bpm |
| **Grain** | per-measurement |
| **Contract** | `contracts/metrics/resting_heart_rate.yml` |

**Description:** Individual BPM readings. Resting heart rate (RHR) is derived as the minimum BPM during sleep or rest periods. Lower RHR generally indicates better cardiovascular fitness. A sudden increase of 5+ BPM above personal baseline may signal illness, overtraining, or stress.

**Thresholds:**

| Range | Label |
|-------|-------|
| 40-55 | Athletic - excellent cardiovascular fitness |
| 56-70 | Normal - healthy range |
| 71-85 | Elevated - monitor for trends |
| >= 86 | High - consult healthcare provider |

**Computations:** daily_value, period_average, trend, intraday

**Related Metrics:**
- sleep_score (lag: 0) — poor sleep elevates next-day resting HR
- readiness_score (lag: 0) — elevated RHR is a key contributor to low readiness
- daily_stress (lag: 0) — chronic stress raises resting heart rate baseline
- workout (lag: 1 day) — intense exercise temporarily elevates RHR for 24-48h

---

#### 22. Heart Rate Variability (HRV)

| Field | Value |
|-------|-------|
| **Name** | `hrv` |
| **Display Name** | Heart Rate Variability |
| **Source** | Oura Ring + Apple Health |
| **Silver Table** | `silver.heart_rate` |
| **Column** | `hrv_ms` |
| **Unit** | ms (SDNN) |
| **Grain** | per-measurement |

**Description:** Time variation between successive heartbeats, measured as SDNN in milliseconds. Higher HRV indicates better autonomic nervous system flexibility and recovery capacity. HRV is highly personal — trends matter more than absolute values. Alcohol, illness, and overtraining significantly suppress HRV.

**Thresholds:** Highly individual. Typical adult range is 20-100ms. Athletic individuals often exceed 60ms.

**Related Metrics:**
- readiness_score (lag: 0) — HRV balance is a key readiness contributor
- sleep_score (lag: 0) — deep sleep promotes HRV recovery
- daily_stress (lag: 0) — chronic stress suppresses HRV

---

#### 9. Blood Pressure

| Field | Value |
|-------|-------|
| **Name** | `blood_pressure` |
| **Display Name** | Blood Pressure |
| **Source** | Withings BPM Connect + Apple Health |
| **Silver Table** | `silver.blood_pressure` |
| **Columns** | `systolic`, `diastolic` |
| **Unit** | mmHg |
| **Grain** | per-measurement |
| **Contract** | `contracts/metrics/blood_pressure.yml` |

**Description:** Systolic and diastolic blood pressure readings. Blood pressure is a critical cardiovascular health marker. Consistent elevation above 130/80 is classified as hypertension by AHA guidelines.

**Thresholds:**

| Systolic / Diastolic | Label |
|----------------------|-------|
| < 120 / < 80 | Optimal - healthy blood pressure |
| 120-129 / < 80 | Elevated - lifestyle changes recommended |
| 130-139 / 80-89 | High Stage 1 - consult healthcare provider |
| >= 140 / >= 90 | High Stage 2 - medical attention needed |

**Computations:** daily_value, period_average, trend, high_readings

**Related Metrics:**
- resting_heart_rate (lag: 0) — high BP and elevated HR co-occur with cardiovascular stress
- weight (lag: 0) — weight gain is associated with blood pressure increase
- daily_stress (lag: 0) — acute stress temporarily raises blood pressure
- activity_score (lag: 30 days) — regular exercise lowers resting blood pressure

---

#### 10. Body Temperature

| Field | Value |
|-------|-------|
| **Name** | `body_temperature` |
| **Display Name** | Body Temperature |
| **Source** | Apple Health + Oura Ring |
| **Silver Table** | `silver.body_temperature` |
| **Column** | `temperature_degc` |
| **Unit** | degC |
| **Grain** | per-measurement |
| **Contract** | `contracts/metrics/body_temperature.yml` |

**Description:** Body temperature in degrees Celsius. Normal range is 36.1-37.2C. Deviations above 37.5C may indicate fever or infection. Oura also tracks temperature deviation from personal baseline as a readiness contributor.

**Thresholds:**

| Range | Label |
|-------|-------|
| 36.1-37.2C | Normal |
| 37.3-38.0C | Low-grade fever - monitor symptoms |
| >= 38.1C | Fever - possible infection, rest recommended |
| <= 36.0C | Below normal - monitor for hypothermia |

**Computations:** daily_value, period_average, trend, elevated_days

---

#### 11. Respiratory Rate

| Field | Value |
|-------|-------|
| **Name** | `respiratory_rate` |
| **Display Name** | Respiratory Rate |
| **Source** | Apple Health |
| **Silver Table** | `silver.respiratory_rate` |
| **Column** | `breaths_per_min` |
| **Unit** | breaths/min |
| **Grain** | per-measurement |
| **Contract** | `contracts/metrics/respiratory_rate.yml` |

**Description:** Breathing rate typically captured during sleep. Normal adult range at rest is 12-20 breaths per minute. Elevated rates during sleep may indicate respiratory infection, sleep apnea, or autonomic stress.

**Thresholds:**

| Range | Label |
|-------|-------|
| 12-16 | Optimal - healthy respiratory rate |
| 17-20 | Normal - within healthy range |
| 21-25 | Elevated - monitor for respiratory issues |
| >= 26 | High - possible illness or respiratory stress |

**Computations:** daily_value, period_average, trend, elevated_days

---

#### 12. Blood Oxygen (SpO2)

| Field | Value |
|-------|-------|
| **Name** | `blood_oxygen` (contract name) / `spo2` |
| **Display Name** | Blood Oxygen (SpO2) |
| **Source** | Oura Ring |
| **Silver Table** | `silver.daily_spo2` |
| **Column** | `spo2_avg_pct` |
| **Unit** | % |
| **Grain** | daily |
| **Contract** | `contracts/metrics/blood_oxygen.yml` |

**Description:** Blood oxygen saturation measured during sleep. Normal values are 95-100%. Consistently low readings below 95% may indicate respiratory issues, sleep apnea, or altitude effects. Provides both average and lowest nightly SpO2 values.

**Thresholds:**

| Range | Label |
|-------|-------|
| >= 95% | Normal - healthy oxygen saturation |
| 90-94% | Low - possible respiratory concern |
| <= 89% | Critical - seek medical attention |

**Computations:** daily_value, period_average, trend, low_nights

---

### Body Composition

#### 7. Body Weight

| Field | Value |
|-------|-------|
| **Name** | `weight` |
| **Display Name** | Body Weight |
| **Source** | Withings Body+ |
| **Silver Table** | `silver.weight` |
| **Column** | `weight_kg` |
| **Unit** | kg |
| **Grain** | per-measurement |
| **Contract** | `contracts/metrics/weight.yml` |

**Description:** Body weight with body composition breakdown via bioelectrical impedance analysis (Withings scale). Includes fat mass, muscle mass, bone mass, and hydration percentage. Best interpreted as a long-term trend — daily fluctuations of 1-2kg are normal.

**Thresholds:** Personal — no universal ranges. Alerts on rapid weekly change (> 1.0 kg/week).

**Computations:** daily_value, period_average, trend, body_composition_trend

**Related Metrics:**
- calories (lag: 14 days) — caloric surplus/deficit drives weight change
- steps (lag: 14 days) — higher step counts contribute to energy expenditure
- protein (lag: 14 days) — adequate protein supports muscle mass retention
- blood_pressure (lag: 0) — weight gain is associated with blood pressure increase

---

#### 8. Body Fat

| Field | Value |
|-------|-------|
| **Name** | `body_fat` |
| **Display Name** | Body Fat |
| **Source** | Withings Body+ |
| **Silver Table** | `silver.weight` |
| **Column** | `fat_mass_kg` |
| **Unit** | kg |
| **Grain** | per-measurement |

**Description:** Fat mass in kilograms measured by the Withings scale via bioelectrical impedance. Tracked within the `weight` table alongside total weight, muscle mass, bone mass, and hydration. Use the `body_composition_trend` computation in the weight contract for longitudinal analysis.

---

### Nutrition

#### 15. Water Intake

| Field | Value |
|-------|-------|
| **Name** | `water_intake` |
| **Display Name** | Water Intake |
| **Source** | Apple Health |
| **Silver Table** | `silver.water_intake` |
| **Column** | `water_ml` |
| **Unit** | ml |
| **Grain** | per-entry (aggregated to daily) |
| **Contract** | `contracts/metrics/water_intake.yml` |

**Description:** Daily water intake logged per drink and aggregated to daily totals. EFSA recommends 2.5L/day for men, 2.0L/day for women (from all sources). Individual needs increase with exercise, heat, and altitude.

**Thresholds:**

| Range | Label |
|-------|-------|
| >= 2000 ml | Excellent - well hydrated |
| 1500-1999 ml | Good - adequate hydration |
| <= 1499 ml | Needs attention - increase water intake |

**Computations:** daily_value, period_average, trend, low_days

---

#### 16. Daily Calories

| Field | Value |
|-------|-------|
| **Name** | `calories` |
| **Display Name** | Daily Calories |
| **Source** | Lifesum |
| **Silver Table** | `silver.daily_meal` |
| **Column** | `calories` |
| **Unit** | kcal |
| **Grain** | per-meal (aggregated to daily) |
| **Contract** | `contracts/metrics/calories.yml` |

**Description:** Total daily caloric intake from food logging. Logged per meal and aggregated to daily totals. Combined with activity data, caloric intake determines energy balance. Typical adult maintenance is 2000-2500 kcal/day.

**Thresholds:**

| Range | Label |
|-------|-------|
| >= 3000 | High intake - surplus for most adults |
| 2000-2999 | Moderate - typical maintenance range |
| 1200-1999 | Low - deficit for most adults |
| <= 1199 | Very low - possible under-logging or restriction |

**Computations:** daily_value, period_average, trend, macro_breakdown

---

#### 17. Daily Protein Intake

| Field | Value |
|-------|-------|
| **Name** | `protein` |
| **Display Name** | Daily Protein Intake |
| **Source** | Lifesum |
| **Silver Table** | `silver.daily_meal` |
| **Column** | `protein` |
| **Unit** | grams |
| **Grain** | per-meal (aggregated to daily) |
| **Contract** | `contracts/metrics/protein.yml` |

**Description:** Daily protein intake. Sports nutrition recommends 1.6-2.2g per kg bodyweight for active individuals. Higher intake is especially important during caloric deficit to preserve lean mass.

**Thresholds:**

| Range | Label |
|-------|-------|
| >= 130g | Excellent - supports muscle growth (1.6g+/kg for 80kg) |
| 100-129g | Good - adequate for moderate activity |
| 60-99g | Low - may not support recovery needs |
| <= 59g | Very low - possible under-logging or insufficient intake |

**Computations:** daily_value, period_average, trend, per_kg_bodyweight

---

### Mindfulness

#### 19. Mindfulness Session

| Field | Value |
|-------|-------|
| **Name** | `mindful_session` |
| **Display Name** | Mindfulness Session |
| **Source** | Apple Health |
| **Silver Table** | `silver.mindful_session` |
| **Column** | `duration_seconds` |
| **Unit** | seconds |
| **Grain** | per-session |
| **Contract** | `contracts/metrics/mindful_session.yml` |

**Description:** Meditation and mindfulness sessions. Consistency of practice (frequency) is more important than individual session length for long-term benefits. Associated with reduced stress, improved HRV, better sleep, and enhanced emotional regulation.

**Thresholds:**

| Scope | Range | Label |
|-------|-------|-------|
| Frequency (weekly) | 7 days | Daily practice - excellent |
| Frequency (weekly) | 4-6 days | Regular |
| Frequency (weekly) | 1-3 days | Occasional - consider increasing |
| Session length | >= 20 min | Deep session |
| Session length | 10-19 min | Standard session |
| Session length | < 10 min | Brief session |

**Computations:** daily_value, period_average, trend, streak

---

### Gait

#### 20. Walking Gait Metrics

| Field | Value |
|-------|-------|
| **Name** | `walking_gait` |
| **Display Name** | Walking Gait Metrics |
| **Source** | Apple Health |
| **Silver Table** | `silver.daily_walking_gait` |
| **Columns** | `walking_speed_kmh`, `step_length_cm`, `walking_asymmetry_pct`, `double_support_pct`, `walking_steadiness` |
| **Unit** | multiple |
| **Grain** | daily |
| **Contract** | `contracts/metrics/walking_gait.yml` |

**Description:** Apple Health gait analysis with walking speed, step length, walking asymmetry, double support time, and walking steadiness score. Reflects musculoskeletal health, balance, and fall risk.

**Thresholds:**

| Metric | Range | Label |
|--------|-------|-------|
| Walking speed | >= 4.0 km/h | Healthy |
| Walking speed | <= 3.5 km/h | Below average - possible mobility concern |
| Asymmetry | <= 8% | Normal - symmetrical gait |
| Asymmetry | 9-15% | Mild asymmetry - monitor |
| Asymmetry | >= 16% | High asymmetry - possible injury |
| Double support | 20-30% | Normal |
| Double support | >= 31% | Elevated - reduced balance confidence |

**Computations:** daily_value, period_average, trend, steadiness_distribution

---

### Hygiene

#### 21. Toothbrushing

| Field | Value |
|-------|-------|
| **Name** | `toothbrushing` |
| **Display Name** | Toothbrushing |
| **Source** | Apple Health (smart toothbrush) |
| **Silver Table** | `silver.toothbrushing` |
| **Column** | `duration_seconds` |
| **Unit** | seconds |
| **Grain** | per-session |
| **Contract** | `contracts/metrics/toothbrushing.yml` |

**Description:** Brushing sessions from a connected smart toothbrush. Dental professionals recommend at least 120 seconds (2 minutes) twice daily. Tracks duration and frequency as oral hygiene compliance indicators.

**Thresholds:**

| Scope | Range | Label |
|-------|-------|-------|
| Duration | >= 120s | Good - meets 2-minute recommendation |
| Duration | 60-119s | Short - increase brushing time |
| Duration | <= 59s | Insufficient |
| Frequency | 2x daily | Optimal |
| Frequency | 1x daily | Add second session |
| Frequency | 0 | Missed - no brushing recorded |

**Computations:** daily_value, period_average, trend, compliance, missed_days

---

### Environment

#### 23. Daily Weather

| Field | Value |
|-------|-------|
| **Name** | `weather` |
| **Display Name** | Daily Weather |
| **Source** | Open-Meteo (free API) |
| **Silver Table** | `silver.daily_weather` |
| **Columns** | `temp_max_c`, `temp_min_c`, `precipitation_mm`, `wind_speed_max_kmh`, `uv_index_max` |
| **Unit** | various (C, mm, km/h, index) |
| **Grain** | daily |
| **Contract** | `contracts/metrics/weather.yml` |

**Description:** Daily weather conditions for the local area. Used to correlate outdoor conditions with sleep quality, activity levels, and mood. Temperature and UV index modulate circadian rhythm and vitamin D synthesis.

**Thresholds:**

| Temp Range | Label |
|------------|-------|
| >= 30C | Hot (heat stress risk) |
| 20-29C | Warm (comfortable outdoor) |
| 10-19C | Mild |
| 0-9C | Cool |
| <= -1C | Cold (exposure risk) |

**Computations:** daily_value, period_average, trend

---

### Lab Results

#### 24. Lab Biomarkers

| Field | Value |
|-------|-------|
| **Name** | `lab_biomarkers` |
| **Display Name** | Lab Biomarkers |
| **Source** | Manual (YAML import) |
| **Silver Table** | `silver.lab_results` |
| **Column** | `value_numeric` |
| **Unit** | varies by marker |
| **Grain** | per-test |
| **Contract** | `contracts/metrics/lab_biomarkers.yml` |

**Description:** Periodic lab test results including blood panels (65+ biomarkers) and microbiome analysis (30+ markers). Supports longitudinal tracking across retests. Imported manually via YAML files.

**Key Marker Thresholds:**

| Marker | Optimal | Concern |
|--------|---------|---------|
| Pancreatic elastase | >= 200 | < 100 (severe insufficiency) |
| Secretory IgA | 510-2040 | < 510 (weakened mucosal immunity) |
| EPA | >= 2.0 | < 2.0 (supplement recommended) |
| Vitamin D3 (25-OH) | 40-60 | < 20 (deficiency) |
| Calprotectin | <= 50 | > 50 (possible inflammation) |

**Computations:** specific_marker, out_of_range, test_summary, category_summary, longitudinal

---

## Composite Scores

### Daily Health Score

Defined in `_business_rules.yml`. Weighted composite of three Oura scores:

| Component | Weight |
|-----------|--------|
| Sleep Score | 35% |
| Readiness Score | 35% |
| Activity Score | 30% |

**Interpretation:**

| Score | Status |
|-------|--------|
| >= 85 | Excellent |
| 70-84 | Good |
| 55-69 | Fair |
| < 55 | Poor |

---

## Alerts

Defined in `_business_rules.yml`:

| Alert | Condition | Severity |
|-------|-----------|----------|
| Low sleep | sleep_score < 60 | warning |
| Low readiness | readiness_score < 60 | warning |
| High resting HR | resting_hr > baseline + 5 | warning |
| Low HRV | hrv < baseline * 0.7 | warning |
| Dehydration risk | water_ml < 1500 | info |

---

## Anomaly Detection

Method: z-score over 90-day rolling window.

| Z-Score | Classification |
|---------|---------------|
| > 2.0 | Anomaly |
| > 1.5 | Unusual |
| <= 1.5 | Normal |
