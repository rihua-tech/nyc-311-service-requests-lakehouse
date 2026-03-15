# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Build Dim Agency
# MAGIC
# MAGIC Purpose:
# MAGIC - Build the agency dimension from `silver.agency_reference`.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - `silver.agency_reference`
# MAGIC
# MAGIC Expected outputs:
# MAGIC - `gold.dim_agency`
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Read agency reference rows.
# MAGIC 2. Assign stable starter surrogate keys from business keys.
# MAGIC 3. Persist the dimension.

# COMMAND ----------
from src.common.constants import GOLD_TABLES, SILVER_REFERENCE_TABLES
from src.transformation.gold_dimensions import build_dim_agency

print(f"Scaffold notebook: build {GOLD_TABLES['dim_agency']} from {SILVER_REFERENCE_TABLES['agency']}")

sample_agency_reference_rows = [
    {
        "agency_code": "DEP",
        "agency_name": "Department of Environmental Protection",
        "first_seen_created_date": "2024-01-02",
    },
    {
        "agency_code": "DSNY",
        "agency_name": "Department of Sanitation",
        "first_seen_created_date": "2024-01-01",
    },
]
dim_agency_rows = build_dim_agency(sample_agency_reference_rows)

print(f"Prepared {len(dim_agency_rows)} dim_agency rows")

# TODO: Confirm whether agency history requires slowly changing logic.
