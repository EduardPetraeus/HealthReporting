# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver DLT Pipeline
# MAGIC
# MAGIC Delta Live Tables pipeline for silver layer transformations.
# MAGIC Reads SQL files from the `sql/` directory and executes them as DLT
# MAGIC table definitions. Each SQL file contains the full staging + MERGE
# MAGIC pattern for one silver entity.
# MAGIC
# MAGIC **Runtime globals used:** `spark` (Databricks runtime)

# COMMAND ----------

import os

# Path to the SQL files directory — relative to the notebook location
SQL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql")

# All silver SQL files to execute (excluding template and README)
SILVER_SQL_FILES = [
    "daily_sleep.sql",
    "daily_activity.sql",
    "daily_readiness.sql",
    "heart_rate.sql",
    "oura_heart_rate.sql",
    "blood_pressure.sql",
    "blood_oxygen.sql",
    "weight.sql",
    "daily_meal.sql",
    "daily_annotations.sql",
    # New silver tables
    "step_count.sql",
    "daily_stress.sql",
    "workout.sql",
    "daily_spo2.sql",
    "body_temperature.sql",
    "resting_heart_rate.sql",
    "water_intake.sql",
    "daily_walking_gait.sql",
    "mindful_session.sql",
    "respiratory_rate.sql",
    "toothbrushing.sql",
    "supplement_log.sql",
    "lab_results.sql",
    "daily_energy_by_source.sql",
    "hrv.sql",
    "vo2_max.sql",
    "daily_resilience.sql",
]


def _execute_sql_file(sql_file: str) -> None:
    """Read and execute all statements in a silver SQL file."""
    sql_path = os.path.join(SQL_DIR, sql_file)

    with open(sql_path, encoding="utf-8") as f:
        sql_content = f.read()

    # Split on semicolons, skip empty and comment-only blocks
    statements = [s.strip() for s in sql_content.split(";") if s.strip()]
    for statement in statements:
        # Skip pure comments or COMMAND markers
        lines = [
            ln
            for ln in statement.splitlines()
            if ln.strip() and not ln.strip().startswith("--")
        ]
        if lines:
            spark.sql(statement)  # noqa: F821 — spark is a Databricks runtime global


# COMMAND ----------

# Execute all silver SQL transformations sequentially.
# Each file handles its own CREATE TABLE IF NOT EXISTS, staging, MERGE, and cleanup.
for sql_file in SILVER_SQL_FILES:
    sql_path = os.path.join(SQL_DIR, sql_file)
    if os.path.exists(sql_path):
        print(f"Executing silver transform: {sql_file}")
        _execute_sql_file(sql_file)
        print(f"  Done: {sql_file}")
    else:
        print(f"  Skipping (not found): {sql_file}")

print(f"\nSilver DLT pipeline complete. Processed {len(SILVER_SQL_FILES)} SQL files.")
