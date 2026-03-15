# Databricks notebook source
# MAGIC %md
# MAGIC # 09 Build Mart Backlog Snapshot
# MAGIC
# MAGIC Purpose:
# MAGIC - Publish a backlog snapshot mart for open request monitoring.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - `gold.fact_service_requests`
# MAGIC
# MAGIC Expected outputs:
# MAGIC - `gold.mart_backlog_snapshot`
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Choose a snapshot date for backlog measurement.
# MAGIC 2. Filter request facts that are still open as of that date.
# MAGIC 3. Group by `status_name` and `agency_code`.
# MAGIC 4. Write the snapshot mart.

# COMMAND ----------
from src.common.constants import GOLD_TABLES
from src.transformation.gold_marts import build_backlog_snapshot

print(f"Scaffold notebook: build {GOLD_TABLES['mart_backlog_snapshot']}")

sample_fact_rows = [
    {
        "request_id": "1001",
        "created_date": "2024-01-01",
        "closed_date": None,
        "agency_code": "DEP",
        "status_name": "Open",
        "is_closed": False,
    },
    {
        "request_id": "1002",
        "created_date": "2024-01-01",
        "closed_date": "2024-01-02",
        "agency_code": "DSNY",
        "status_name": "Closed",
        "is_closed": True,
    },
]
mart_rows = build_backlog_snapshot(sample_fact_rows, snapshot_date="2024-01-01")

print(f"Prepared {len(mart_rows)} mart_backlog_snapshot rows")

# TODO: Replace sample rows with a Spark read from `gold.fact_service_requests`.
# TODO: Decide whether snapshot dates should come from a job parameter, schedule date, or control table.
