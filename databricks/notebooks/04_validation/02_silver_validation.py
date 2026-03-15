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
from src.common.utils import parse_optional_datetime
from src.quality.duplicate_checks import duplicate_summary
from src.quality.null_checks import null_counts_by_field
from src.quality.rowcount_checks import reconciliation_summary
from src.quality.schema_checks import type_mismatches
from src.transformation.silver_service_requests import BOROUGH_NORMALIZATION_MAP

print(f"Scaffold notebook: validate {SILVER_TABLE}")

sample_silver_rows: list[dict[str, object]] = []
required_silver_fields = ["request_id", "created_at", "created_date", "record_hash"]
valid_boroughs = set(BOROUGH_NORMALIZATION_MAP.values())
expected_silver_types = {
    "request_id": str,
    "created_at": str,
    "created_date": str,
    "resolution_time_hours": (float, int),
}

required_field_nulls = null_counts_by_field(sample_silver_rows, required_silver_fields)
duplicate_request_ids = duplicate_summary(sample_silver_rows, "request_id")
timestamp_issues = {
    field: sum(
        1
        for row in sample_silver_rows
        if row.get(field) not in (None, "") and parse_optional_datetime(row.get(field)) is None
    )
    for field in ("created_at", "closed_at", "updated_at", "due_date")
}
unexpected_boroughs = sorted(
    {
        str(row.get("borough"))
        for row in sample_silver_rows
        if row.get("borough") not in (None, "") and row.get("borough") not in valid_boroughs
    }
)
simple_type_issues = [
    {
        "request_id": row.get("request_id"),
        "issues": type_mismatches(row, expected_silver_types),
    }
    for row in sample_silver_rows
    if type_mismatches(row, expected_silver_types)
]
negative_resolution_hours = sum(
    1
    for row in sample_silver_rows
    if row.get("resolution_time_hours") not in (None, "") and float(row["resolution_time_hours"]) < 0
)
silver_row_summary = reconciliation_summary(
    expected_count=len(sample_silver_rows),
    actual_count=len(sample_silver_rows) - duplicate_request_ids["duplicate_row_count"],
    comparison_name="silver_duplicate_adjusted_rowcount",
)

print(
    "Scaffold silver validation summary: "
    f"required_field_nulls={required_field_nulls}, "
    f"duplicate_request_ids={duplicate_request_ids}, "
    f"timestamp_issues={timestamp_issues}, "
    f"unexpected_boroughs={unexpected_boroughs}, "
    f"simple_type_issues={simple_type_issues}, "
    f"negative_resolution_hours={negative_resolution_hours}, "
    f"silver_row_summary={silver_row_summary}"
)

# TODO: Replace sample rows with Spark reads from `silver.service_requests_clean`.
# TODO: Persist validation results once monitoring and operational ownership are defined.
