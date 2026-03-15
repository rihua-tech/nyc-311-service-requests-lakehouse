# Databricks notebook source
# MAGIC %md
# MAGIC # 08 Build Mart Service Performance
# MAGIC
# MAGIC Purpose:
# MAGIC - Publish a performance mart for closure and resolution analytics.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - `gold.fact_service_requests`
# MAGIC
# MAGIC Expected outputs:
# MAGIC - `gold.mart_service_performance`
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Filter to closed request facts.
# MAGIC 2. Aggregate by `agency_code` and `complaint_type`.
# MAGIC 3. Calculate closed request counts and average resolution time.
# MAGIC 4. Write the mart.

# COMMAND ----------
from src.common.constants import GOLD_TABLES
from src.transformation.gold_marts import build_service_performance

print(f"Scaffold notebook: build {GOLD_TABLES['mart_service_performance']}")

sample_fact_rows = [
    {
        "agency_code": "DSNY",
        "complaint_type": "Missed Collection",
        "resolution_time_hours": 2.0,
        "is_closed": True,
    },
    {
        "agency_code": "DSNY",
        "complaint_type": "Missed Collection",
        "resolution_time_hours": 4.0,
        "is_closed": True,
    },
    {
        "agency_code": "DEP",
        "complaint_type": "Water System",
        "resolution_time_hours": None,
        "is_closed": False,
    },
]
mart_rows = build_service_performance(sample_fact_rows)

print(f"Prepared {len(mart_rows)} mart_service_performance rows")

# TODO: Replace sample rows with a Spark read from `gold.fact_service_requests`.
# TODO: Join gold dimensions in Databricks if richer business labels are needed downstream.
