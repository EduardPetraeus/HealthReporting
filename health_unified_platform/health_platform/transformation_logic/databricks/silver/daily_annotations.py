# Databricks notebook source
# Source: manually curated — no bronze source table.
# daily_annotations is a hand-maintained reference table for training events, incidents, travel, etc.
# Insert rows directly into health_dw.silver.daily_annotations as needed.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS health_dw.silver.daily_annotations (
# MAGIC   sk_date         INT     NOT NULL,
# MAGIC   annotation_type STRING  NOT NULL,
# MAGIC   annotation      STRING  NOT NULL,
# MAGIC   created_by      STRING,
# MAGIC   is_valid        BOOLEAN NOT NULL
# MAGIC )
# MAGIC USING DELTA;
