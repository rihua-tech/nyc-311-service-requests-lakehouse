"""Project-wide constants used by starter modules."""

BRONZE_TABLE = "bronze.nyc311_service_requests_raw"
SILVER_TABLE = "silver.service_requests_clean"

SILVER_REFERENCE_TABLES = {
    "agency": "silver.agency_reference",
    "complaint_type": "silver.complaint_type_reference",
    "location": "silver.location_reference",
    "status": "silver.status_reference",
}

GOLD_TABLES = {
    "dim_date": "gold.dim_date",
    "dim_agency": "gold.dim_agency",
    "dim_complaint_type": "gold.dim_complaint_type",
    "dim_location": "gold.dim_location",
    "dim_status": "gold.dim_status",
    "fact_service_requests": "gold.fact_service_requests",
    "mart_request_volume_daily": "gold.mart_request_volume_daily",
    "mart_service_performance": "gold.mart_service_performance",
    "mart_backlog_snapshot": "gold.mart_backlog_snapshot",
}

SOURCE_SYSTEM = "nyc_311_api"

