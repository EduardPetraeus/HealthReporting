# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## One-Time Platform Initialisation
# MAGIC
# MAGIC Run this notebook **once** before the first pipeline execution.
# MAGIC It creates the Unity Catalog schemas (bronze, silver, gold, audit)
# MAGIC and the audit Delta tables (job_runs, table_runs) + overview view.
# MAGIC
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
schemas = ["bronze", "silver", "gold", "audit"]

for schema in schemas:
    fqn = f"{catalog}.{schema}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fqn}")
    print(f"Schema ready: {fqn}")

print("\nAll schemas initialised.")

# COMMAND ----------

# Create audit tables and view (idempotent — uses IF NOT EXISTS / OR REPLACE)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS `{catalog}`.audit.job_runs (
        job_id           STRING  NOT NULL,
        job_name         STRING  NOT NULL,
        job_type         STRING  NOT NULL,
        source_system    STRING,
        env              STRING,
        start_time       TIMESTAMP,
        end_time         TIMESTAMP,
        duration_seconds DOUBLE,
        status           STRING,
        error_message    STRING,
        rows_processed   LONG,
        rows_inserted    LONG,
        rows_updated     LONG,
        rows_deleted     LONG
    )
    USING DELTA
""")
print("Table ready: audit.job_runs")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS `{catalog}`.audit.table_runs (
        run_id        STRING NOT NULL,
        job_id        STRING,
        table_name    STRING NOT NULL,
        layer         STRING,
        operation     STRING,
        rows_before   LONG,
        rows_after    LONG,
        rows_changed  LONG,
        start_time    TIMESTAMP,
        end_time      TIMESTAMP,
        status        STRING,
        error_message STRING
    )
    USING DELTA
""")
print("Table ready: audit.table_runs")

spark.sql(f"""
    CREATE OR REPLACE VIEW `{catalog}`.audit.v_platform_overview AS
    WITH last_success AS (
        SELECT source_system, job_type, MAX(end_time) AS last_successful_run
        FROM `{catalog}`.audit.job_runs
        WHERE status = 'success'
        GROUP BY source_system, job_type
    ),
    week_stats AS (
        SELECT
            source_system,
            job_type,
            COUNT_IF(status = 'success')   AS success_count,
            COUNT_IF(status = 'error')     AS error_count,
            COUNT(*)                       AS total_count,
            SUM(rows_processed)            AS total_rows_processed
        FROM `{catalog}`.audit.job_runs
        WHERE start_time >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
        GROUP BY source_system, job_type
    )
    SELECT
        ls.source_system,
        ls.job_type,
        ls.last_successful_run,
        COALESCE(ws.success_count, 0)                                         AS success_7d,
        COALESCE(ws.error_count, 0)                                           AS error_7d,
        COALESCE(ws.total_count, 0)                                           AS total_runs_7d,
        ROUND(100.0 * COALESCE(ws.success_count, 0)
              / NULLIF(COALESCE(ws.total_count, 0), 0), 1)                    AS success_rate_pct,
        COALESCE(ws.total_rows_processed, 0)                                  AS total_rows_7d
    FROM last_success ls
    LEFT JOIN week_stats ws
        ON ls.source_system = ws.source_system
        AND ls.job_type = ws.job_type
    ORDER BY ls.source_system, ls.job_type
""")
print("View ready: audit.v_platform_overview")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Verify

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {catalog}"))
display(spark.sql(f"SHOW TABLES IN `{catalog}`.audit"))
