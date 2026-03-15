# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Build Dim Date
# MAGIC
# MAGIC Purpose:
# MAGIC - Build the gold date dimension used by downstream facts and marts.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - distinct `created_date` and `closed_date` values from `silver.service_requests_clean`
# MAGIC
# MAGIC Expected outputs:
# MAGIC - `gold.dim_date`
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Collect distinct date values from the silver request dataset.
# MAGIC 2. Generate calendar attributes such as quarter, weekday, and weekend flags.
# MAGIC 3. Write the date dimension to Delta.

# COMMAND ----------
from src.common.constants import GOLD_TABLES, SILVER_TABLE
from src.transformation.gold_dimensions import build_dim_date, collect_date_values

print(f"Scaffold notebook: build {GOLD_TABLES['dim_date']} from {SILVER_TABLE}")

sample_silver_rows = [
    {"created_date": "2024-01-01", "closed_date": "2024-01-02"},
    {"created_date": "2024-01-03", "closed_date": None},
]
date_values = collect_date_values(sample_silver_rows)
dim_date_rows = build_dim_date(date_values)

print(f"Prepared {len(dim_date_rows)} dim_date rows")

# TODO: Replace sample rows with a Spark read from `silver.service_requests_clean`.
# TODO: Decide whether `dim_date` should be fully regenerated or incrementally maintained.
