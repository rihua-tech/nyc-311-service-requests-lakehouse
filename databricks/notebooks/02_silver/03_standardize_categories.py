# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Standardize Categories
# MAGIC
# MAGIC Purpose:
# MAGIC - Extract starter agency, complaint type, and status reference tables from cleaned silver rows.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - cleaned silver request rows after core field selection and deduplication
# MAGIC
# MAGIC Expected outputs:
# MAGIC - starter rows for `silver.agency_reference`, `silver.complaint_type_reference`, and `silver.status_reference`
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Review agency, complaint, and status fields from the silver table.
# MAGIC 2. Apply light normalization where values are obviously blank or inconsistent.
# MAGIC 3. Materialize reference-table starter datasets.

# COMMAND ----------
from src.transformation.silver_reference_tables import (
    build_agency_reference,
    build_complaint_type_reference,
    build_status_reference,
)

sample_silver_rows: list[dict[str, object]] = []
agency_rows = build_agency_reference(sample_silver_rows)
complaint_rows = build_complaint_type_reference(sample_silver_rows)
status_rows = build_status_reference(sample_silver_rows)

print(
    "Scaffold notebook: reference row counts "
    f"(agency={len(agency_rows)}, complaint_type={len(complaint_rows)}, status={len(status_rows)})"
)

# TODO: Add source-driven mapping rules only after profiling real category variance.
