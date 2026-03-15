# Databricks notebook source
# MAGIC %md
# MAGIC # 09 Build Mart Backlog Snapshot
# MAGIC
# MAGIC Purpose:
# MAGIC - Publish a backlog snapshot mart for open request monitoring.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - `gold.fact_service_requests`
# MAGIC
# MAGIC Expected outputs:
# MAGIC - `gold.mart_backlog_snapshot`
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Filter open requests.
# MAGIC 2. Group by snapshot date and status or agency.
# MAGIC 3. Write the mart.

# COMMAND ----------
print("Scaffold notebook: build gold.mart_backlog_snapshot")

# TODO: Decide how snapshot dates should be materialized in production.

