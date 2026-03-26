# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Gold Validation
# MAGIC
# MAGIC Purpose:
# MAGIC - Run starter validation checks for gold dimensions, facts, and marts.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - gold dimensions, fact tables, and reporting marts
# MAGIC
# MAGIC Expected outputs:
# MAGIC - scaffolded uniqueness, reconciliation, and aggregate sanity checks
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Validate dimension uniqueness and null-safe business keys.
# MAGIC 2. Reconcile fact row counts with `silver.service_requests_clean`.
# MAGIC 3. Spot-check mart aggregates once gold logic is implemented.

# COMMAND ----------
from pyspark.sql import functions as F

from src.common.constants import GOLD_TABLES, SILVER_TABLE
from src.common.databricks_runtime import bootstrap_notebook, get_widget

bootstrap_notebook(
    spark=spark,  # type: ignore[name-defined]
    dbutils=dbutils,  # type: ignore[name-defined]
    extra_widget_defaults={"snapshot_date": ""},
)

silver_df = spark.table(SILVER_TABLE)  # type: ignore[name-defined]
fact_df = spark.table(GOLD_TABLES["fact_service_requests"])  # type: ignore[name-defined]
request_volume_df = spark.table(GOLD_TABLES["mart_request_volume_daily"])  # type: ignore[name-defined]
service_performance_df = spark.table(GOLD_TABLES["mart_service_performance"])  # type: ignore[name-defined]
backlog_df = spark.table(GOLD_TABLES["mart_backlog_snapshot"])  # type: ignore[name-defined]
snapshot_date = get_widget(dbutils, "snapshot_date", default="")  # type: ignore[name-defined]
snapshot_expr = F.to_date(F.lit(snapshot_date)) if snapshot_date else F.current_date()

duplicate_dim_checks = {
    "dim_agency": spark.table(GOLD_TABLES["dim_agency"]).groupBy("agency_code").count().filter("agency_code IS NOT NULL AND count > 1").count(),  # type: ignore[name-defined]
    "dim_complaint_type": spark.table(GOLD_TABLES["dim_complaint_type"]).groupBy("complaint_type", "descriptor").count().filter("complaint_type IS NOT NULL AND count > 1").count(),  # type: ignore[name-defined]
    "dim_location": spark.table(GOLD_TABLES["dim_location"]).groupBy("incident_zip", "borough", "city", "location_type").count().filter("incident_zip IS NOT NULL AND count > 1").count(),  # type: ignore[name-defined]
    "dim_status": spark.table(GOLD_TABLES["dim_status"]).groupBy("status_name").count().filter("status_name IS NOT NULL AND count > 1").count(),  # type: ignore[name-defined]
}

expected_open_count = fact_df.filter(
    (F.col("created_date") <= snapshot_expr)
    & (
        (~F.col("is_closed"))
        | (F.col("is_closed") & F.col("closed_date").isNotNull() & (F.col("closed_date") > snapshot_expr))
    )
).count()

summary = {
    "silver_row_count": silver_df.count(),
    "gold_fact_row_count": fact_df.count(),
    "request_volume_total": request_volume_df.agg(F.sum("request_count")).collect()[0][0] or 0,
    "closed_request_total": service_performance_df.agg(F.sum("closed_requests")).collect()[0][0] or 0,
    "expected_closed_requests": fact_df.filter(F.col("is_closed")).count(),
    "backlog_total": backlog_df.agg(F.sum("open_request_count")).collect()[0][0] or 0,
    "expected_backlog_total": expected_open_count,
    "duplicate_dimension_business_keys": duplicate_dim_checks,
}

print(summary)

if (
    summary["gold_fact_row_count"] != summary["silver_row_count"]
    or summary["request_volume_total"] != summary["gold_fact_row_count"]
    or summary["closed_request_total"] != summary["expected_closed_requests"]
    or summary["backlog_total"] != summary["expected_backlog_total"]
    or any(duplicate_dim_checks.values())
):
    raise ValueError("Gold validation failed. Review the printed summary before continuing.")
