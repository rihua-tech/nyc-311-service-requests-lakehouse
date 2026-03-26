# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Bronze Validation
# MAGIC
# MAGIC Purpose:
# MAGIC - Run starter checks against bronze ingestion output and landing metadata.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - `bronze.nyc311_service_requests_raw`
# MAGIC
# MAGIC Expected outputs:
# MAGIC - row counts, metadata completeness checks, raw payload presence checks, and duplicate signals
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Validate required ingestion metadata such as `ingest_id`, `record_hash`, and `file_path`.
# MAGIC 2. Confirm `raw_payload` is present and non-empty.
# MAGIC 3. Inspect duplicate `record_hash` values and summarize bronze row counts.

# COMMAND ----------
from src.common.constants import BRONZE_TABLE
from src.common.databricks_runtime import bootstrap_notebook

bootstrap_notebook(spark=spark, dbutils=dbutils)  # type: ignore[name-defined]

if not spark.catalog.tableExists(BRONZE_TABLE):  # type: ignore[name-defined]
    raise ValueError(f"{BRONZE_TABLE} does not exist. Run the bronze notebooks first.")

bronze_df = spark.table(BRONZE_TABLE)  # type: ignore[name-defined]
required_fields = [
    "ingest_id",
    "source_system",
    "source_record_id",
    "ingest_timestamp",
    "raw_payload",
    "record_hash",
    "file_path",
]

metadata_nulls = {
    field: bronze_df.filter(f"{field} IS NULL OR trim(cast({field} AS STRING)) = ''").count()
    for field in required_fields
}
duplicate_hash_df = bronze_df.groupBy("record_hash").count().filter("record_hash IS NOT NULL AND count > 1")
duplicate_hash_rows = sum(row["count"] - 1 for row in duplicate_hash_df.collect())
raw_payload_presence = bronze_df.filter("raw_payload IS NOT NULL AND trim(raw_payload) != ''").count()

summary = {
    "bronze_row_count": bronze_df.count(),
    "metadata_nulls": metadata_nulls,
    "duplicate_record_hash_rows": duplicate_hash_rows,
    "raw_payload_present_rows": raw_payload_presence,
}

print(summary)

if any(metadata_nulls.values()) or duplicate_hash_rows or raw_payload_presence != summary["bronze_row_count"]:
    raise ValueError("Bronze validation failed. Review the printed summary before continuing.")
