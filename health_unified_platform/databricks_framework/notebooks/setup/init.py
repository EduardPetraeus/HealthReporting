# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## One-Time Platform Initialisation
# MAGIC
# MAGIC Run this notebook **once** before the first pipeline execution.
# MAGIC It creates the Unity Catalog schemas (bronze, silver, gold).
# MAGIC Silver and gold tables are created lazily by the SQL files themselves
# MAGIC (`CREATE TABLE IF NOT EXISTS`) so no DDL is needed here per entity.
# MAGIC
# MAGIC **Requirements**
# MAGIC - The `health_dw` catalog must already exist (create via Databricks UI
# MAGIC   or with `CREATE CATALOG IF NOT EXISTS health_dw` if you have metastore admin rights).
# MAGIC - You need `CREATE SCHEMA` privilege on the catalog.

# COMMAND ----------

dbutils.widgets.text("catalog", "health_dw", "Catalog")
catalog = dbutils.widgets.get("catalog").strip()

# COMMAND ----------

# Create schemas — safe to re-run at any time.
schemas = ["bronze", "silver", "gold"]

for schema in schemas:
    fqn = f"{catalog}.{schema}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fqn}")
    print(f"Schema ready: {fqn}")

print("\nAll schemas initialised.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Verify

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {catalog}"))
