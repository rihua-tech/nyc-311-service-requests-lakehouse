# Databricks notebook source
# MAGIC %md
# MAGIC # 05 Build Dim Status
# MAGIC
# MAGIC Purpose:
# MAGIC - Build the request status dimension.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - `silver.status_reference`
# MAGIC
# MAGIC Expected outputs:
# MAGIC - `gold.dim_status`
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Read status reference data.
# MAGIC 2. Assign surrogate keys.
# MAGIC 3. Persist the dimension.

# COMMAND ----------
print("Scaffold notebook: build gold.dim_status")

# TODO: Confirm whether status groupings are needed for reporting.
