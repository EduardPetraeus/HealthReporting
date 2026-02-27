# Databricks notebook source
# Gold view: valid daily annotations (training events, incidents, travel, etc.)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW health_dw.gold.vw_daily_annotations_valid AS
# MAGIC SELECT
# MAGIC     sk_date,
# MAGIC     annotation_type AS activity_type,
# MAGIC     annotation      AS comment
# MAGIC FROM health_dw.silver.daily_annotations
# MAGIC WHERE is_valid = TRUE;
