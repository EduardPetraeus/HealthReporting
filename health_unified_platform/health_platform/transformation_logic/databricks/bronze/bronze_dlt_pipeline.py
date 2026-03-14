# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bronze DLT Pipeline
# MAGIC
# MAGIC Delta Live Tables pipeline for all bronze source ingestion.
# MAGIC Uses Auto Loader (cloudFiles) to incrementally ingest parquet files
# MAGIC from cloud storage into bronze Delta tables.
# MAGIC
# MAGIC Each source gets its own @dlt.table() function for independent
# MAGIC scheduling, monitoring, and error isolation.
# MAGIC
# MAGIC **Runtime globals used:** `spark` (Databricks runtime)

# COMMAND ----------

# Landing zone base path — override via pipeline settings if needed
import os as _os  # noqa: E402 — needed for env var fallback

import dlt
from pyspark.sql.functions import current_timestamp, input_file_name, lit

_STORAGE_ACCOUNT = _os.environ.get("HEALTH_STORAGE_ACCOUNT", "healthdatalake")

LANDING_ZONE = spark.conf.get(  # noqa: F821 — spark is a Databricks runtime global
    "health.landing_zone",
    f"abfss://landing@{_STORAGE_ACCOUNT}.dfs.core.windows.net",
)

CHECKPOINT_ROOT = spark.conf.get(  # noqa: F821
    "health.checkpoint_root",
    f"abfss://checkpoints@{_STORAGE_ACCOUNT}.dfs.core.windows.net/bronze",
)

SCHEMA_ROOT = spark.conf.get(  # noqa: F821
    "health.schema_root",
    f"abfss://checkpoints@{_STORAGE_ACCOUNT}.dfs.core.windows.net/bronze/schemas",
)


def _read_cloud_files(
    source_name: str, relative_path: str, file_format: str = "parquet"
):
    """Helper: read from cloud storage using Auto Loader with schema inference."""
    source_path = f"{LANDING_ZONE}/{relative_path}"
    schema_location = f"{SCHEMA_ROOT}/{source_name}"

    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", file_format)
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(source_path)
        .withColumn("_source_file", input_file_name())
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("source_system", lit(source_name.split("_")[0]))
    )


# =============================================================================
# Apple Health sources
# =============================================================================


@dlt.table(
    name="stg_apple_health_step_count",
    comment="Raw step count data from Apple Health",
)
def stg_apple_health_step_count():
    return _read_cloud_files(
        "apple_health_step_count", "apple_health_data/Activity/stepcount/**/*.parquet"
    )


@dlt.table(
    name="stg_apple_health_heart_rate",
    comment="Raw heart rate data from Apple Health",
)
def stg_apple_health_heart_rate():
    return _read_cloud_files(
        "apple_health_heart_rate", "apple_health_data/Vitality/heartrate/**/*.parquet"
    )


@dlt.table(
    name="stg_apple_health_vo2_max",
    comment="Raw VO2 Max data from Apple Health",
)
def stg_apple_health_vo2_max():
    return _read_cloud_files(
        "apple_health_vo2_max", "apple_health_data/Vitality/vo2max/**/*.parquet"
    )


@dlt.table(
    name="stg_apple_health_water",
    comment="Raw water intake data from Apple Health",
)
def stg_apple_health_water():
    return _read_cloud_files(
        "apple_health_water", "apple_health_data/Nutrition/dietarywater/**/*.parquet"
    )


@dlt.table(
    name="stg_apple_health_body_temperature",
    comment="Raw body temperature data from Apple Health",
)
def stg_apple_health_body_temperature():
    return _read_cloud_files(
        "apple_health_body_temperature",
        "apple_health_data/Vitality/bodytemperature/**/*.parquet",
    )


@dlt.table(
    name="stg_apple_health_respiratory_rate",
    comment="Raw respiratory rate data from Apple Health",
)
def stg_apple_health_respiratory_rate():
    return _read_cloud_files(
        "apple_health_respiratory_rate",
        "apple_health_data/Vitality/respiratoryrate/**/*.parquet",
    )


@dlt.table(
    name="stg_apple_health_walking_steadiness",
    comment="Raw walking steadiness data from Apple Health",
)
def stg_apple_health_walking_steadiness():
    return _read_cloud_files(
        "apple_health_walking_steadiness",
        "apple_health_data/Mobility/applewalkingsteadiness/**/*.parquet",
    )


@dlt.table(
    name="stg_apple_health_walking_asymmetry",
    comment="Raw walking asymmetry data from Apple Health",
)
def stg_apple_health_walking_asymmetry():
    return _read_cloud_files(
        "apple_health_walking_asymmetry",
        "apple_health_data/Mobility/walkingasymmetrypercentage/**/*.parquet",
    )


@dlt.table(
    name="stg_apple_health_walking_double_support",
    comment="Raw walking double support data from Apple Health",
)
def stg_apple_health_walking_double_support():
    return _read_cloud_files(
        "apple_health_walking_double_support",
        "apple_health_data/Mobility/walkingdoublesupportpercentage/**/*.parquet",
    )


@dlt.table(
    name="stg_apple_health_walking_heart_rate_avg",
    comment="Raw walking heart rate average from Apple Health",
)
def stg_apple_health_walking_heart_rate_avg():
    return _read_cloud_files(
        "apple_health_walking_heart_rate_avg",
        "apple_health_data/Mobility/walkingheartrateaverage/**/*.parquet",
    )


@dlt.table(
    name="stg_apple_health_walking_speed",
    comment="Raw walking speed data from Apple Health",
)
def stg_apple_health_walking_speed():
    return _read_cloud_files(
        "apple_health_walking_speed",
        "apple_health_data/Mobility/walkingspeed/**/*.parquet",
    )


@dlt.table(
    name="stg_apple_health_walking_step_length",
    comment="Raw walking step length data from Apple Health",
)
def stg_apple_health_walking_step_length():
    return _read_cloud_files(
        "apple_health_walking_step_length",
        "apple_health_data/Mobility/walkingsteplength/**/*.parquet",
    )


@dlt.table(
    name="stg_apple_health_mindful_session",
    comment="Raw mindful session data from Apple Health",
)
def stg_apple_health_mindful_session():
    return _read_cloud_files(
        "apple_health_mindful_session",
        "apple_health_data/Mindfulness/mindfulsession/**/*.parquet",
    )


@dlt.table(
    name="stg_apple_health_toothbrushing",
    comment="Raw toothbrushing event data from Apple Health",
)
def stg_apple_health_toothbrushing():
    return _read_cloud_files(
        "apple_health_toothbrushing",
        "apple_health_data/Hygiene/toothbrushingevent/**/*.parquet",
    )


@dlt.table(
    name="stg_apple_health_hrv",
    comment="Raw heart rate variability (SDNN) data from Apple Health",
)
def stg_apple_health_hrv():
    return _read_cloud_files(
        "apple_health_hrv",
        "apple_health_data/Vitality/heartratevariabilitysdnn/**/*.parquet",
    )


@dlt.table(
    name="stg_apple_health_resting_heart_rate",
    comment="Raw resting heart rate data from Apple Health",
)
def stg_apple_health_resting_heart_rate():
    return _read_cloud_files(
        "apple_health_resting_heart_rate",
        "apple_health_data/Other/restingheartrate/**/*.parquet",
    )


@dlt.table(
    name="stg_apple_health_active_energy_burned",
    comment="Raw active energy burned data from Apple Health",
)
def stg_apple_health_active_energy_burned():
    return _read_cloud_files(
        "apple_health_active_energy_burned",
        "apple_health_data/Activity/activeenergyburned/**/*.parquet",
    )


@dlt.table(
    name="stg_apple_health_basal_energy_burned",
    comment="Raw basal energy burned data from Apple Health",
)
def stg_apple_health_basal_energy_burned():
    return _read_cloud_files(
        "apple_health_basal_energy_burned",
        "apple_health_data/Activity/basalenergyburned/**/*.parquet",
    )


# =============================================================================
# Oura Ring sources
# =============================================================================


@dlt.table(
    name="stg_oura_daily_sleep",
    comment="Raw daily sleep data from Oura Ring API",
)
def stg_oura_daily_sleep():
    return _read_cloud_files("oura_daily_sleep", "oura/raw/daily_sleep/**/*.parquet")


@dlt.table(
    name="stg_oura_daily_activity",
    comment="Raw daily activity data from Oura Ring API",
)
def stg_oura_daily_activity():
    return _read_cloud_files(
        "oura_daily_activity", "oura/raw/daily_activity/**/*.parquet"
    )


@dlt.table(
    name="stg_oura_daily_readiness",
    comment="Raw daily readiness data from Oura Ring API",
)
def stg_oura_daily_readiness():
    return _read_cloud_files(
        "oura_daily_readiness", "oura/raw/daily_readiness/**/*.parquet"
    )


@dlt.table(
    name="stg_oura_heartrate",
    comment="Raw heart rate data from Oura Ring API",
)
def stg_oura_heartrate():
    return _read_cloud_files("oura_heartrate", "oura/raw/heartrate/**/*.parquet")


@dlt.table(
    name="stg_oura_workout",
    comment="Raw workout data from Oura Ring API",
)
def stg_oura_workout():
    return _read_cloud_files("oura_workout", "oura/raw/workout/**/*.parquet")


@dlt.table(
    name="stg_oura_daily_spo2",
    comment="Raw daily SpO2 data from Oura Ring API",
)
def stg_oura_daily_spo2():
    return _read_cloud_files("oura_daily_spo2", "oura/raw/daily_spo2/**/*.parquet")


@dlt.table(
    name="stg_oura_daily_stress",
    comment="Raw daily stress data from Oura Ring API",
)
def stg_oura_daily_stress():
    return _read_cloud_files("oura_daily_stress", "oura/raw/daily_stress/**/*.parquet")


@dlt.table(
    name="stg_oura_daily_resilience",
    comment="Raw daily resilience data from Oura Ring API",
)
def stg_oura_daily_resilience():
    return _read_cloud_files(
        "oura_daily_resilience", "oura/raw/daily_resilience/**/*.parquet"
    )


@dlt.table(
    name="stg_oura_vo2_max",
    comment="Raw VO2 max data from Oura Ring API",
)
def stg_oura_vo2_max():
    return _read_cloud_files("oura_vo2_max", "oura/raw/vo2_max/**/*.parquet")


# =============================================================================
# Withings sources
# =============================================================================


@dlt.table(
    name="stg_withings_weight",
    comment="Raw weight and body composition data from Withings",
)
def stg_withings_weight():
    return _read_cloud_files("withings_weight", "withings/raw/weight/**/*.parquet")


@dlt.table(
    name="stg_withings_blood_pressure",
    comment="Raw blood pressure data from Withings",
)
def stg_withings_blood_pressure():
    return _read_cloud_files(
        "withings_blood_pressure", "withings/raw/blood_pressure/**/*.parquet"
    )


@dlt.table(
    name="stg_withings_temperature",
    comment="Raw temperature data from Withings",
)
def stg_withings_temperature():
    return _read_cloud_files(
        "withings_temperature", "withings/raw/temperature/**/*.parquet"
    )


# =============================================================================
# Strava sources
# =============================================================================


@dlt.table(
    name="stg_strava_activities",
    comment="Raw activity data from Strava",
)
def stg_strava_activities():
    return _read_cloud_files("strava_activities", "strava/raw/activities/**/*.parquet")


# =============================================================================
# Lifesum sources
# =============================================================================


@dlt.table(
    name="stg_lifesum_food",
    comment="Raw food logging data from Lifesum",
)
def stg_lifesum_food():
    return _read_cloud_files("lifesum_food", "lifesum/parquet/food/*.parquet")


# =============================================================================
# Weather sources
# =============================================================================


@dlt.table(
    name="stg_weather_open_meteo",
    comment="Raw weather data from Open-Meteo API",
)
def stg_weather_open_meteo():
    return _read_cloud_files(
        "weather_open_meteo", "weather/raw/open_meteo/**/*.parquet"
    )
