# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Silver Validation
# MAGIC
# MAGIC Purpose:
# MAGIC - Run starter checks against silver curated datasets.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - `silver.service_requests_clean`
# MAGIC
# MAGIC Expected outputs:
# MAGIC - null, duplicate, timestamp, borough, and numeric-quality check results
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Validate required fields such as `request_id`, `created_at`, and `created_date`.
# MAGIC 2. Inspect duplicate `request_id` values and basic row-count reconciliation.
# MAGIC 3. Check timestamp parseability, borough normalization, and non-negative `resolution_time_hours`.

# COMMAND ----------
from src.common.constants import SILVER_TABLE
from src.common.databricks_runtime import bootstrap_notebook
from src.transformation.silver_service_requests import BOROUGH_NORMALIZATION_MAP

bootstrap_notebook(spark=spark, dbutils=dbutils)  # type: ignore[name-defined]

if not spark.catalog.tableExists(SILVER_TABLE):  # type: ignore[name-defined]
    raise ValueError(f"{SILVER_TABLE} does not exist. Run the silver notebooks first.")

silver_df = spark.table(SILVER_TABLE)  # type: ignore[name-defined]
required_fields = ["request_id", "created_at", "created_date", "record_hash"]
required_nulls = {
    field: silver_df.filter(f"{field} IS NULL OR trim(cast({field} AS STRING)) = ''").count()
    for field in required_fields
}
duplicate_request_id_df = silver_df.groupBy("request_id").count().filter("request_id IS NOT NULL AND count > 1")
duplicate_request_id_rows = sum(row["count"] - 1 for row in duplicate_request_id_df.collect())
unexpected_boroughs = sorted(
    row["borough"]
    for row in silver_df.select("borough").distinct().collect()
    if row["borough"] not in (None, "") and row["borough"] not in set(BOROUGH_NORMALIZATION_MAP.values())
)
negative_resolution_hours = silver_df.filter("resolution_time_hours < 0").count()

summary = {
    "silver_row_count": silver_df.count(),
    "required_nulls": required_nulls,
    "duplicate_request_id_rows": duplicate_request_id_rows,
    "unexpected_boroughs": unexpected_boroughs,
    "negative_resolution_hours": negative_resolution_hours,
}

print(summary)

if any(required_nulls.values()) or duplicate_request_id_rows or unexpected_boroughs or negative_resolution_hours:
    raise ValueError("Silver validation failed. Review the printed summary before continuing.")
