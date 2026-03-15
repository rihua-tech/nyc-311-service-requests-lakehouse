# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Build Dim Location
# MAGIC
# MAGIC Purpose:
# MAGIC - Build a starter location dimension for borough and ZIP-based analysis.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - `silver.location_reference`
# MAGIC
# MAGIC Expected outputs:
# MAGIC - `gold.dim_location`
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Read location reference rows.
# MAGIC 2. Standardize business keys.
# MAGIC 3. Write the dimension.

# COMMAND ----------
print("Scaffold notebook: build gold.dim_location")

# TODO: Decide whether geospatial enrichment belongs in silver or gold.

