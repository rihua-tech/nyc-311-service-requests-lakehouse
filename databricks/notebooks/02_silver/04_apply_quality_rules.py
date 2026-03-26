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
from src.common.constants import BRONZE_TABLE, SILVER_TABLE
from src.common.databricks_runtime import bootstrap_notebook
from src.transformation.silver_service_requests import BOROUGH_NORMALIZATION_MAP

config = bootstrap_notebook(spark=spark, dbutils=dbutils)  # type: ignore[name-defined]

if not spark.catalog.tableExists(SILVER_TABLE):  # type: ignore[name-defined]
    raise ValueError(f"{SILVER_TABLE} does not exist. Run the silver notebooks first.")

silver_df = spark.table(SILVER_TABLE)  # type: ignore[name-defined]
bronze_count = spark.table(BRONZE_TABLE).count() if spark.catalog.tableExists(BRONZE_TABLE) else None  # type: ignore[name-defined]

required_fields = ["request_id", "created_at", "created_date", "record_hash"]
required_nulls = {
    field: silver_df.filter(f"{field} IS NULL").count()
    for field in required_fields
}

duplicate_request_ids = (
    silver_df.filter("request_id IS NOT NULL")
    .groupBy("request_id")
    .count()
    .filter("count > 1")
)
duplicate_row_count = sum(row["count"] - 1 for row in duplicate_request_ids.collect())

unexpected_boroughs = [
    row["borough"]
    for row in silver_df.select("borough").distinct().collect()
    if row["borough"] not in (None, "") and row["borough"] not in set(BOROUGH_NORMALIZATION_MAP.values())
]
negative_resolution_hours = silver_df.filter("resolution_time_hours < 0").count()

summary = {
    "silver_row_count": silver_df.count(),
    "bronze_row_count": bronze_count,
    "required_nulls": required_nulls,
    "duplicate_request_id_rows": duplicate_row_count,
    "unexpected_boroughs": sorted(unexpected_boroughs),
    "negative_resolution_hours": negative_resolution_hours,
}

print(summary)

if any(required_nulls.values()) or duplicate_row_count or unexpected_boroughs or negative_resolution_hours:
    raise ValueError("Silver quality rules failed. Review the printed summary before continuing.")
