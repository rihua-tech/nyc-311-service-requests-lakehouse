# Databricks notebook source
# MAGIC %md
# MAGIC # 07 Build Mart Request Volume Daily
# MAGIC
# MAGIC Purpose:
# MAGIC - Publish a daily request volume mart for trend reporting.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - `gold.fact_service_requests`
# MAGIC
# MAGIC Expected outputs:
# MAGIC - `gold.mart_request_volume_daily`
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Aggregate requests by created date.
# MAGIC 2. Join calendar attributes if needed.
# MAGIC 3. Write the mart.

# COMMAND ----------
print("Scaffold notebook: build gold.mart_request_volume_daily")

# TODO: Confirm grain and reporting filters with dashboard requirements.

