# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Clean Service Requests
# MAGIC
# MAGIC Purpose:
# MAGIC - Standardize core service request fields from bronze to silver.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - `bronze.nyc311_service_requests_raw`
# MAGIC - bronze rows with `raw_payload`, `record_hash`, and ingest metadata
# MAGIC
# MAGIC Expected outputs:
# MAGIC - starter rows for `silver.service_requests_clean`
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Read bronze rows from Delta.
# MAGIC 2. Parse `raw_payload` and select core service request fields.
# MAGIC 3. Cast timestamps, normalize borough values, and derive operational fields.
# MAGIC 4. Remove duplicate `request_id` values and write silver output.

# COMMAND ----------
from src.common.constants import BRONZE_TABLE, SILVER_TABLE
from src.transformation.silver_service_requests import clean_service_requests

print(f"Scaffold notebook: transform {BRONZE_TABLE} into {SILVER_TABLE}")

sample_bronze_rows: list[dict[str, object]] = []
preview_rows = clean_service_requests(sample_bronze_rows)

print(f"Prepared preview rows: {len(preview_rows)}")

# TODO: Read bronze rows from Spark and translate the helper stages into DataFrame expressions.
# TODO: Persist the final DataFrame to `silver.service_requests_clean`.
