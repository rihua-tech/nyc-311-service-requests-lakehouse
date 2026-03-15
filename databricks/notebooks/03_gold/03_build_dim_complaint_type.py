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
# MAGIC 2. Deduplicate complaint type and descriptor pairs and assign surrogate keys.
# MAGIC 3. Write the dimension.

# COMMAND ----------
from src.common.constants import GOLD_TABLES, SILVER_REFERENCE_TABLES
from src.transformation.gold_dimensions import build_dim_complaint_type

print(
    f"Scaffold notebook: build {GOLD_TABLES['dim_complaint_type']} "
    f"from {SILVER_REFERENCE_TABLES['complaint_type']}"
)

sample_complaint_reference_rows = [
    {
        "complaint_type": "Noise",
        "descriptor": "Loud Music",
        "first_seen_created_date": "2024-01-01",
    },
    {
        "complaint_type": "Water System",
        "descriptor": "Leak",
        "first_seen_created_date": "2024-01-02",
    },
]
dim_complaint_type_rows = build_dim_complaint_type(sample_complaint_reference_rows)

print(f"Prepared {len(dim_complaint_type_rows)} dim_complaint_type rows")

# TODO: Finalize descriptor handling before dimension columns are locked in.
