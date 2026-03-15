# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Bronze Validation
# MAGIC
# MAGIC Purpose:
# MAGIC - Run starter checks against bronze ingestion output.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - `bronze.nyc311_service_requests_raw`
# MAGIC
# MAGIC Expected outputs:
# MAGIC - row counts, duplicate signals, and metadata checks
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Count bronze rows.
# MAGIC 2. Inspect duplicate hashes.
# MAGIC 3. Review ingest timestamp coverage.

# COMMAND ----------
print("Scaffold notebook: validate bronze layer")

# TODO: Add threshold-based assertions after load patterns are known.

