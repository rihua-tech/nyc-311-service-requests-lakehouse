# Databricks notebook source
# MAGIC %md
# MAGIC # 06 Build Fact Service Requests
# MAGIC
# MAGIC Purpose:
# MAGIC - Build the request-level fact table for gold reporting models.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - `silver.service_requests_clean`
# MAGIC - `gold.dim_agency`
# MAGIC - `gold.dim_complaint_type`
# MAGIC - `gold.dim_location`
# MAGIC - `gold.dim_status`
# MAGIC
# MAGIC Expected outputs:
# MAGIC - `gold.fact_service_requests`
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Read silver requests.
# MAGIC 2. Resolve surrogate keys from gold dimensions.
# MAGIC 3. Select fact measures, dates, and degenerate request attributes.
# MAGIC 4. Write the fact table.

# COMMAND ----------
from src.common.constants import GOLD_TABLES, SILVER_TABLE
from src.transformation.gold_dimensions import (
    build_dim_agency,
    build_dim_complaint_type,
    build_dim_location,
    build_dim_status,
)
from src.transformation.gold_facts import build_fact_service_requests

print(f"Scaffold notebook: build {GOLD_TABLES['fact_service_requests']} from {SILVER_TABLE}")

sample_silver_rows = [
    {
        "request_id": "1001",
        "created_at": "2024-01-01T10:00:00+00:00",
        "closed_at": "2024-01-01T12:00:00+00:00",
        "due_date": "2024-01-01T18:00:00+00:00",
        "created_date": "2024-01-01",
        "closed_date": "2024-01-01",
        "agency_code": "DSNY",
        "agency_name": "Department of Sanitation",
        "complaint_type": "Missed Collection",
        "descriptor": "Collection missed",
        "status_name": "Closed",
        "incident_zip": "11201",
        "borough": "BROOKLYN",
        "city": "BROOKLYN",
        "location_type": "Residential Building",
        "latitude": 40.6931,
        "longitude": -73.9897,
        "channel_type": "Phone",
        "resolution_time_hours": 2.0,
        "is_closed": True,
        "is_overdue": False,
        "record_hash": "sample-hash",
        "load_timestamp": "2024-01-01T13:00:00+00:00",
    }
]

dim_agency_rows = build_dim_agency(sample_silver_rows)
dim_complaint_type_rows = build_dim_complaint_type(sample_silver_rows)
dim_location_rows = build_dim_location(sample_silver_rows)
dim_status_rows = build_dim_status(sample_silver_rows)
fact_rows = build_fact_service_requests(
    sample_silver_rows,
    dim_agency_rows=dim_agency_rows,
    dim_complaint_type_rows=dim_complaint_type_rows,
    dim_location_rows=dim_location_rows,
    dim_status_rows=dim_status_rows,
)

print(f"Prepared {len(fact_rows)} fact_service_requests rows")

# TODO: Replace sample rows with Spark DataFrame reads from silver and gold Delta tables.
# TODO: Translate the lookup helpers into explicit DataFrame joins in Databricks.
