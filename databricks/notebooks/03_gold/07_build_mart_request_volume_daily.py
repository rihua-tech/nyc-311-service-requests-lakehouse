# Databricks notebook source
# MAGIC %md
# MAGIC # 07 Build Mart Request Volume Daily
# MAGIC
# MAGIC Purpose:
# MAGIC - Publish a daily request volume mart for trend reporting.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - `gold.fact_service_requests`
# MAGIC
# MAGIC Expected outputs:
# MAGIC - `gold.mart_request_volume_daily`
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Read request-level fact rows.
# MAGIC 2. Aggregate request counts by `created_date`.
# MAGIC 3. Write the daily volume mart.

# COMMAND ----------
from src.common.constants import GOLD_TABLES
from src.transformation.gold_marts import build_request_volume_daily

print(f"Scaffold notebook: build {GOLD_TABLES['mart_request_volume_daily']}")

sample_fact_rows = [
    {"request_id": "1001", "created_date": "2024-01-01"},
    {"request_id": "1002", "created_date": "2024-01-01"},
    {"request_id": "1003", "created_date": "2024-01-02"},
]
mart_rows = build_request_volume_daily(sample_fact_rows)

print(f"Prepared {len(mart_rows)} mart_request_volume_daily rows")

# TODO: Replace sample rows with a Spark read from `gold.fact_service_requests`.
# TODO: Join `gold.dim_date` in Databricks if calendar enrichment is needed in the published table.
