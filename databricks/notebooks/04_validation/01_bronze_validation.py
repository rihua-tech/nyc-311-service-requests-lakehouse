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
from src.quality.duplicate_checks import duplicate_summary
from src.quality.null_checks import null_counts_by_field
from src.quality.rowcount_checks import reconciliation_summary

print(f"Scaffold notebook: validate {BRONZE_TABLE}")

sample_bronze_rows: list[dict[str, object]] = []
required_bronze_fields = [
    "ingest_id",
    "source_system",
    "source_record_id",
    "ingest_timestamp",
    "raw_payload",
    "record_hash",
    "file_path",
]

metadata_nulls = null_counts_by_field(sample_bronze_rows, required_bronze_fields)
record_hash_summary = duplicate_summary(sample_bronze_rows, "record_hash")
raw_payload_presence = reconciliation_summary(
    expected_count=len(sample_bronze_rows),
    actual_count=sum(1 for row in sample_bronze_rows if row.get("raw_payload") not in (None, "")),
    comparison_name="bronze_raw_payload_presence",
)

print(
    "Scaffold bronze validation summary: "
    f"metadata_nulls={metadata_nulls}, "
    f"record_hash_summary={record_hash_summary}, "
    f"raw_payload_presence={raw_payload_presence}"
)

# TODO: Replace sample rows with Spark reads from `bronze.nyc311_service_requests_raw`.
# TODO: Add Databricks-specific alerting or failed-batch handling once runtime behavior is defined.
