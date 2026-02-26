# Databricks Silver SQL

**Status: 1 / 18 silver entities implemented** — work in progress.

One SQL file per silver entity. Files are referenced by source YAML configs and executed by `silver_runner.py`.

Multiple sources can share the same SQL file if their bronze schemas are normalised to the same columns (e.g. `apple_health_heart_rate` and `oura_heart_rate` both use `heart_rate.sql`).

## Implemented
- `heart_rate.sql`

## Remaining
daily_sleep, daily_activity, daily_readiness, daily_spo2, daily_stress, workout, personal_info, step_count, toothbrushing, daily_meal, daily_walking_gait, mindful_session, body_temperature, respiratory_rate, water_intake, daily_energy_by_source, daily_spo2
