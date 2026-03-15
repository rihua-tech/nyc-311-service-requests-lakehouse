# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Build Dim Complaint Type
# MAGIC
# MAGIC Purpose:
# MAGIC - Build the complaint type dimension for reporting slices.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - `silver.complaint_type_reference`
# MAGIC
# MAGIC Expected outputs:
# MAGIC - `gold.dim_complaint_type`
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Read complaint reference rows.
# MAGIC 2. Deduplicate business keys.
# MAGIC 3. Write the dimension.

# COMMAND ----------
print("Scaffold notebook: build gold.dim_complaint_type")

# TODO: Finalize descriptor handling before dimension columns are locked in.

