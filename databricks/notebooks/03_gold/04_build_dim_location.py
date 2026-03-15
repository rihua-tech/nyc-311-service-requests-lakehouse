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
# MAGIC 2. Standardize location business keys and assign surrogate keys.
# MAGIC 3. Write the dimension.

# COMMAND ----------
from src.common.constants import GOLD_TABLES, SILVER_REFERENCE_TABLES
from src.transformation.gold_dimensions import build_dim_location

print(f"Scaffold notebook: build {GOLD_TABLES['dim_location']} from {SILVER_REFERENCE_TABLES['location']}")

sample_location_reference_rows = [
    {
        "incident_zip": "11201",
        "borough": "BROOKLYN",
        "city": "BROOKLYN",
        "location_type": "Residential Building",
        "latitude": 40.6931,
        "longitude": -73.9897,
    },
    {
        "incident_zip": "11101",
        "borough": "QUEENS",
        "city": "LONG ISLAND CITY",
        "location_type": "Street",
        "latitude": 40.7447,
        "longitude": -73.9485,
    },
]
dim_location_rows = build_dim_location(sample_location_reference_rows)

print(f"Prepared {len(dim_location_rows)} dim_location rows")

# TODO: Decide whether geospatial enrichment belongs in silver or gold.
