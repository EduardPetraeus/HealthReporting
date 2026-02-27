# Databricks Silver SQL

One SQL file per silver entity. Files use `-- COMMAND ----------` as Databricks SQL notebook cell separator.

Two types of SQL files coexist here:

**YAML-driven templates** — use `{source_system}`, `{bronze_table}`, `{silver_table}` placeholders, executed by `silver_runner.py`. Multiple sources can share the same file when bronze schemas are normalised to the same columns.

**Standalone transforms** — hardcoded source/target tables, executed directly as Databricks SQL notebooks. Ported from `archive/legacy_databricks_pipeline/`. Will migrate to YAML-driven pattern when bronze layer is ready.

## YAML-driven templates
- `heart_rate.sql` — shared by `apple_health_heart_rate` + `oura_heart_rate`

## Standalone transforms (legacy port)
- `blood_oxygen.sql` — Oura SpO2 levels (`workspace.default.oura_blood_oxygen_level`)
- `blood_pressure.sql` — Withings blood pressure (`workspace.default.withings_blood_pressure`)
- `daily_activity.sql` — Oura daily activity (`workspace.default.oura_daily_activity`)
- `daily_annotations.sql` — Manually curated annotations (no source table, insert-only)
- `daily_meal.sql` — Lifesum food entries (`workspace.default.lifesum_food`)
- `daily_readiness.sql` — Oura daily readiness (`workspace.default.oura_daily_readiness`)
- `daily_sleep.sql` — Oura daily sleep (`workspace.default.oura_daily_sleep`)
- `oura_heart_rate.sql` — Oura continuous heart rate (`workspace.default.oura_heart_rate`)
- `weight.sql` — Withings body composition (`workspace.default.withings_weight`)

## TODO: Update source references
All standalone files currently read from `workspace.default.*` (legacy). Update to `health_dw.bronze.stg_*` when bronze autoloader is configured.

## Remaining (not yet implemented)
daily_spo2, daily_stress, workout, personal_info, step_count, toothbrushing,
daily_walking_gait, mindful_session, body_temperature, respiratory_rate,
water_intake, daily_energy_by_source
