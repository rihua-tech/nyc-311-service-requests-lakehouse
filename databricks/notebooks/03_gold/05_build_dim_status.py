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
# MAGIC 2. Assign surrogate keys and retain starter status grouping fields.
# MAGIC 3. Persist the dimension.

# COMMAND ----------
from src.common.constants import GOLD_TABLES, SILVER_REFERENCE_TABLES
from src.transformation.gold_dimensions import build_dim_status

print(f"Scaffold notebook: build {GOLD_TABLES['dim_status']} from {SILVER_REFERENCE_TABLES['status']}")

sample_status_reference_rows = [
    {"status_name": "Closed", "status_group": "Closed", "is_closed_status": True},
    {"status_name": "Open", "status_group": "Open", "is_closed_status": False},
]
dim_status_rows = build_dim_status(sample_status_reference_rows)

print(f"Prepared {len(dim_status_rows)} dim_status rows")

# TODO: Confirm whether status groupings are needed for reporting.
