# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bronze Autoloader
# MAGIC
# MAGIC Generic, config-driven bronze ingestion notebook.
# MAGIC Reads a YAML source config and runs Autoloader (cloudFiles) in either
# MAGIC **incremental** (streaming, trigger-once semantics) or **full** (batch overwrite) mode.
# MAGIC
# MAGIC **Widget parameters**
# MAGIC | Parameter | Description |
# MAGIC |-----------|-------------|
# MAGIC | `source_name` | Config file name without `.yml` extension, e.g. `apple_health_heart_rate` |
# MAGIC | `config_root` | Workspace path containing source YAML configs |

# COMMAND ----------

dbutils.widgets.text("source_name", "", "Source Name")
dbutils.widgets.text(
    "config_root",
    "/Workspace/Shared/health_platform/config/sources",
    "Config Root",
)

source_name = dbutils.widgets.get("source_name").strip()
config_root = dbutils.widgets.get("config_root").strip()

if not source_name:
    raise ValueError("Widget 'source_name' must not be empty.")

# COMMAND ----------

import yaml
from pyspark.sql.functions import current_timestamp, lit

config_path = f"{config_root}/{source_name}.yml"
print(f"Loading config: {config_path}")

with open(config_path) as f:
    config = yaml.safe_load(f)

source_system   = config["source_system"]
bronze_cfg      = config["bronze"]["autoloader"]

source_path          = bronze_cfg["source_path"]
file_format          = bronze_cfg["format"]
checkpoint_location  = bronze_cfg["checkpoint_location"]
schema_location      = bronze_cfg.get("schema_location", checkpoint_location + "/schema")
target_table         = bronze_cfg["target_table"]
load_mode            = bronze_cfg.get("load_mode", "incremental")
extra_options        = bronze_cfg.get("options", {}) or {}

print(f"Source:        {source_name}")
print(f"Source system: {source_system}")
print(f"Load mode:     {load_mode}")
print(f"Source path:   {source_path}")
print(f"Target table:  {target_table}")

# COMMAND ----------

if load_mode == "incremental":
    # -------------------------------------------------------------------
    # Incremental: streaming Autoloader with trigger.availableNow=True
    # Processes all new files since the last checkpoint, then exits.
    # -------------------------------------------------------------------
    reader_options = {
        "cloudFiles.format":          file_format,
        "cloudFiles.schemaLocation":  schema_location,
        "cloudFiles.inferColumnTypes": "true",
        **{f"cloudFiles.{k}" if not k.startswith("cloudFiles.") else k: v
           for k, v in extra_options.items()},
    }

    df = (
        spark.readStream
        .format("cloudFiles")
        .options(**reader_options)
        .load(source_path)
        .withColumn("source_system", lit(source_system))
        .withColumn("_ingested_at", current_timestamp())
    )

    (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_location)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(target_table)
        .awaitTermination()
    )

elif load_mode == "full":
    # -------------------------------------------------------------------
    # Full: batch read and overwrite.
    # Re-reads all files and replaces the entire bronze table.
    # Use for sources where incremental detection is not reliable.
    # -------------------------------------------------------------------
    df = (
        spark.read
        .format(file_format)
        .options(**extra_options)
        .load(source_path)
        .withColumn("source_system", lit(source_system))
        .withColumn("_ingested_at", current_timestamp())
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(target_table)
    )

else:
    raise ValueError(
        f"Unknown load_mode '{load_mode}' in config '{source_name}'. "
        "Expected 'incremental' or 'full'."
    )

print(f"Bronze load complete: {source_name} -> {target_table}")
