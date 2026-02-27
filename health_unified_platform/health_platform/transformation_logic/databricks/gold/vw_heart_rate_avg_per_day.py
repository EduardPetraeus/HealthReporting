# Databricks notebook source
# Gold view: average heart rate per day

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW health_dw.gold.vw_heart_rate_avg_per_day AS
# MAGIC SELECT
# MAGIC     sk_date,
# MAGIC     AVG(bpm) AS avg_bpm
# MAGIC FROM health_dw.silver.heart_rate
# MAGIC GROUP BY sk_date;
