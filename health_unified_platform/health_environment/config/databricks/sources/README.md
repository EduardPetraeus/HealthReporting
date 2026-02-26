# Databricks Source Configs

**Status: 2 / 27 sources configured** — work in progress.

One YAML file per source. Each file covers both bronze ingestion (Autoloader) and silver transformation (SQL reference + merge key).

See `docs/databricks_framework.md` for the full YAML schema and how to add a new source.

## Configured
- `apple_health_heart_rate.yml`
- `oura_heart_rate.yml`

## Remaining (from sources_config.yaml)
Apple Health: toothbrushing, step_count, vo2_max, water, body_temperature, respiratory_rate, walking_gait (6 metrics), mindful_session, height, physical_effort, basal_energy, active_energy

Oura: daily_sleep, daily_activity, daily_readiness, workout, daily_spo2, daily_stress, personal_info
