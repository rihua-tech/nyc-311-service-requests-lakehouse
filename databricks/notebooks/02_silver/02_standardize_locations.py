# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Standardize Locations
# MAGIC
# MAGIC Purpose:
# MAGIC - Validate normalized location fields and extract a starter location reference table.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - silver request rows after core cleaning in `01_clean_service_requests`
# MAGIC
# MAGIC Expected outputs:
# MAGIC - reviewed location attributes and starter rows for `silver.location_reference`
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Review borough normalization results from the silver transform module.
# MAGIC 2. Validate ZIP, city, and coordinate coverage.
# MAGIC 3. Build a starter `silver.location_reference` dataset.

# COMMAND ----------
from src.transformation.silver_reference_tables import build_location_reference

sample_silver_rows: list[dict[str, object]] = []
location_reference_rows = build_location_reference(sample_silver_rows)

print(f"Scaffold notebook: built {len(location_reference_rows)} location reference rows")

# TODO: Add Spark DataFrame logic for ZIP normalization and optional geospatial enrichment.
