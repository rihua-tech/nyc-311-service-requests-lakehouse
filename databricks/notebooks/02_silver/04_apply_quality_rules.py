# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Apply Quality Rules
# MAGIC
# MAGIC Purpose:
# MAGIC - Apply starter data quality checks against the cleaned silver request dataset.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - deduplicated `silver.service_requests_clean`
# MAGIC - optional silver reference tables for spot checks
# MAGIC
# MAGIC Expected outputs:
# MAGIC - null, duplicate, and row-count summaries for the silver batch
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Check required fields such as `request_id` and `created_date`.
# MAGIC 2. Confirm duplicate `request_id` values were removed.
# MAGIC 3. Capture row-count summaries before publishing or validating downstream.

# COMMAND ----------
from src.quality.duplicate_checks import find_duplicate_keys
from src.quality.null_checks import count_nulls

sample_silver_rows: list[dict[str, object]] = []

print(
    "Scaffold notebook: silver quality checks "
    f"(null_request_ids={count_nulls(sample_silver_rows, 'request_id')}, "
    f"duplicate_request_ids={len(find_duplicate_keys(sample_silver_rows, 'request_id'))})"
)

# TODO: Add Spark-based metrics capture and failed-row handling once runtime behavior is finalized.
