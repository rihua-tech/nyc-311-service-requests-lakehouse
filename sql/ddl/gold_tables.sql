CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key INT COMMENT 'Calendar date key in YYYYMMDD form.',
    calendar_date DATE COMMENT 'Calendar date from the silver layer.',
    calendar_year INT COMMENT 'Calendar year.',
    calendar_quarter INT COMMENT 'Calendar quarter.',
    calendar_month INT COMMENT 'Calendar month number.',
    month_name STRING COMMENT 'Calendar month name.',
    calendar_day INT COMMENT 'Day of month.',
    day_of_week INT COMMENT 'ISO day of week where Monday = 1.',
    day_name STRING COMMENT 'Day name.',
    is_weekend BOOLEAN COMMENT 'Starter weekend flag.'
)
USING DELTA;

CREATE TABLE IF NOT EXISTS gold.dim_agency (
    agency_sk INT COMMENT 'Starter surrogate key generated sequentially from sorted agency codes.',
    agency_code STRING COMMENT 'Agency code from silver.agency_reference.',
    agency_name STRING COMMENT 'Agency display name.',
    first_seen_created_date DATE COMMENT 'Earliest created date seen in the silver reference batch.',
    effective_timestamp TIMESTAMP COMMENT 'Timestamp when the dimension row was produced.'
)
USING DELTA;

CREATE TABLE IF NOT EXISTS gold.dim_complaint_type (
    complaint_type_sk INT COMMENT 'Starter surrogate key generated sequentially from sorted complaint type and descriptor pairs.',
    complaint_type STRING COMMENT 'Complaint type from silver.complaint_type_reference.',
    descriptor STRING COMMENT 'Complaint descriptor paired with the complaint type.',
    first_seen_created_date DATE COMMENT 'Earliest created date seen in the silver reference batch.',
    effective_timestamp TIMESTAMP COMMENT 'Timestamp when the dimension row was produced.'
)
USING DELTA;

CREATE TABLE IF NOT EXISTS gold.dim_location (
    location_sk INT COMMENT 'Starter surrogate key generated sequentially from sorted location business keys.',
    incident_zip STRING COMMENT 'Incident ZIP code.',
    borough STRING COMMENT 'Canonical borough value from the silver layer.',
    city STRING COMMENT 'City or locality value.',
    location_type STRING COMMENT 'Location type from the silver layer.',
    latitude DOUBLE COMMENT 'Representative latitude from the silver reference row.',
    longitude DOUBLE COMMENT 'Representative longitude from the silver reference row.',
    effective_timestamp TIMESTAMP COMMENT 'Timestamp when the dimension row was produced.'
)
USING DELTA;

CREATE TABLE IF NOT EXISTS gold.dim_status (
    status_sk INT COMMENT 'Starter surrogate key generated sequentially from sorted status names.',
    status_name STRING COMMENT 'Normalized status name from silver.status_reference.',
    status_group STRING COMMENT 'Starter grouping such as Open or Closed.',
    is_closed_status BOOLEAN COMMENT 'Boolean flag indicating whether the status represents closure.',
    effective_timestamp TIMESTAMP COMMENT 'Timestamp when the dimension row was produced.'
)
USING DELTA;

CREATE TABLE IF NOT EXISTS gold.fact_service_requests (
    request_id STRING COMMENT 'Degenerate business key retained from silver.service_requests_clean.',
    created_date_key INT COMMENT 'Date key derived from created_date; aligns with gold.dim_date.date_key.',
    closed_date_key INT COMMENT 'Date key derived from closed_date; aligns with gold.dim_date.date_key when populated.',
    agency_sk INT COMMENT 'Starter surrogate key from gold.dim_agency. Uses 0 when no match is found.',
    complaint_type_sk INT COMMENT 'Starter surrogate key from gold.dim_complaint_type. Uses 0 when no match is found.',
    location_sk INT COMMENT 'Starter surrogate key from gold.dim_location. Uses 0 when no match is found.',
    status_sk INT COMMENT 'Starter surrogate key from gold.dim_status. Uses 0 when no match is found.',
    created_at TIMESTAMP COMMENT 'Created timestamp from the silver request row.',
    closed_at TIMESTAMP COMMENT 'Closed timestamp from the silver request row.',
    due_date TIMESTAMP COMMENT 'Due date from the silver request row.',
    created_date DATE COMMENT 'Created date retained for scaffold-stage traceability and simple downstream queries.',
    closed_date DATE COMMENT 'Closed date retained for scaffold-stage traceability.',
    agency_code STRING COMMENT 'Agency code retained alongside the surrogate key.',
    complaint_type STRING COMMENT 'Complaint type retained alongside the surrogate key.',
    descriptor STRING COMMENT 'Descriptor retained to support complaint-type dimensional joins.',
    status_name STRING COMMENT 'Status name retained alongside the surrogate key.',
    incident_zip STRING COMMENT 'Incident ZIP retained alongside the location surrogate key.',
    borough STRING COMMENT 'Canonical borough retained alongside the location surrogate key.',
    city STRING COMMENT 'City retained alongside the location surrogate key.',
    location_type STRING COMMENT 'Location type retained alongside the location surrogate key.',
    channel_type STRING COMMENT 'Submission channel retained from silver.',
    resolution_time_hours DOUBLE COMMENT 'Resolution duration in hours for closed requests.',
    is_closed BOOLEAN COMMENT 'Operational closure flag from silver.',
    is_overdue BOOLEAN COMMENT 'Operational overdue flag from silver.',
    record_hash STRING COMMENT 'Lineage hash propagated from silver.',
    load_timestamp TIMESTAMP COMMENT 'Timestamp when the fact row was produced.'
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

-- TODO: Replace scaffold-stage unknown-key handling and retained business keys with final warehouse standards if the project matures.
