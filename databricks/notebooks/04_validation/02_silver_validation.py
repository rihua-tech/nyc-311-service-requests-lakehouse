# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Silver Validation
# MAGIC
# MAGIC Purpose:
# MAGIC - Run starter checks against silver curated datasets.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - `silver.service_requests_clean`
# MAGIC
# MAGIC Expected outputs:
# MAGIC - null, duplicate, and schema check results
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Validate required fields.
# MAGIC 2. Inspect duplicate request ids.
# MAGIC 3. Compare row counts with bronze.

# COMMAND ----------
print("Scaffold notebook: validate silver layer")

# TODO: Add exception handling and metric persistence if monitoring is introduced.

