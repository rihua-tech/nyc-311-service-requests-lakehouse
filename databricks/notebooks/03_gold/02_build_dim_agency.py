# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Build Dim Agency
# MAGIC
# MAGIC Purpose:
# MAGIC - Build the agency dimension from silver reference data.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - `silver.agency_reference`
# MAGIC
# MAGIC Expected outputs:
# MAGIC - `gold.dim_agency`
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Read agency reference rows.
# MAGIC 2. Assign surrogate keys.
# MAGIC 3. Persist the dimension.

# COMMAND ----------
print("Scaffold notebook: build gold.dim_agency")

# TODO: Confirm whether agency history requires slowly changing logic.

