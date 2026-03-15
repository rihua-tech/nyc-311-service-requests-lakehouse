# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Build Dim Date
# MAGIC
# MAGIC Purpose:
# MAGIC - Build the gold date dimension used by reporting marts.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - distinct dates from silver request records
# MAGIC
# MAGIC Expected outputs:
# MAGIC - `gold.dim_date`
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Collect distinct date values.
# MAGIC 2. Generate calendar attributes.
# MAGIC 3. Write the dimension table.

# COMMAND ----------
print("Scaffold notebook: build gold.dim_date")

# TODO: Decide whether `dim_date` should be static or regenerated on demand.

