CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key INT,
    calendar_date DATE,
    calendar_year INT,
    calendar_month INT,
    calendar_day INT
)
USING DELTA;

CREATE TABLE IF NOT EXISTS gold.dim_agency (
    agency_sk INT,
    agency_code STRING,
    agency_name STRING,
    effective_timestamp TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS gold.dim_complaint_type (
    complaint_type_sk INT,
    complaint_type STRING,
    descriptor STRING,
    effective_timestamp TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS gold.dim_location (
    location_sk INT,
    incident_zip STRING,
    borough STRING,
    city STRING,
    effective_timestamp TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS gold.dim_status (
    status_sk INT,
    status_name STRING,
    status_group STRING,
    effective_timestamp TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS gold.fact_service_requests (
    request_id STRING,
    created_date DATE,
    closed_date DATE,
    agency_code STRING,
    complaint_type STRING,
    status_name STRING,
    incident_zip STRING,
    resolution_time_hours DOUBLE,
    is_closed BOOLEAN,
    is_overdue BOOLEAN
)
USING DELTA;

CREATE TABLE IF NOT EXISTS gold.mart_request_volume_daily (
    created_date DATE,
    request_count BIGINT
)
USING DELTA;

CREATE TABLE IF NOT EXISTS gold.mart_service_performance (
    agency_code STRING,
    complaint_type STRING,
    closed_requests BIGINT,
    avg_resolution_time_hours DOUBLE
)
USING DELTA;

CREATE TABLE IF NOT EXISTS gold.mart_backlog_snapshot (
    snapshot_date DATE,
    status_name STRING,
    agency_code STRING,
    open_request_count BIGINT
)
USING DELTA;

-- TODO: Replace business-key placeholders with finalized surrogate-key joins in the fact table.

