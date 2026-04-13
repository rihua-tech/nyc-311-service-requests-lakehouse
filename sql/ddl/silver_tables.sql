-- Replace <storage-account> with the target ADLS account before running this DDL in Databricks.
CREATE SCHEMA IF NOT EXISTS silver
LOCATION 'abfss://curated@<storage-account>.dfs.core.windows.net/silver';

CREATE TABLE IF NOT EXISTS silver.service_requests_clean (
    request_id STRING COMMENT 'Business key derived from the NYC 311 unique request id.',
    created_at TIMESTAMP COMMENT 'Parsed request creation timestamp from bronze raw_payload.',
    closed_at TIMESTAMP COMMENT 'Parsed request closure timestamp from bronze raw_payload.',
    updated_at TIMESTAMP COMMENT 'Parsed latest request update timestamp when available.',
    due_date TIMESTAMP COMMENT 'Parsed due date used for starter overdue logic.',
    agency_code STRING COMMENT 'Agency code from the source payload.',
    agency_name STRING COMMENT 'Agency display name from the source payload.',
    complaint_type STRING COMMENT 'Top-level complaint category.',
    descriptor STRING COMMENT 'Complaint descriptor or subtype.',
    status_name STRING COMMENT 'Current request status after basic normalization.',
    borough STRING COMMENT 'Canonical borough value derived from source text.',
    incident_zip STRING COMMENT 'ZIP code associated with the incident location.',
    city STRING COMMENT 'City or locality field from the source payload.',
    latitude DOUBLE COMMENT 'Parsed latitude from the source payload.',
    longitude DOUBLE COMMENT 'Parsed longitude from the source payload.',
    location_type STRING COMMENT 'Location type such as street or residential building.',
    channel_type STRING COMMENT 'Submission channel when provided by the source.',
    resolution_description STRING COMMENT 'Free-text resolution or action description.',
    resolution_time_hours DOUBLE COMMENT 'Hours between created_at and closed_at for closed requests.',
    is_closed BOOLEAN COMMENT 'Starter operational flag based on status and closed_at.',
    is_overdue BOOLEAN COMMENT 'Starter flag for open requests past their due_date.',
    created_year INT COMMENT 'Derived year from created_at for partition-friendly querying.',
    created_month INT COMMENT 'Derived month from created_at for partition-friendly querying.',
    created_date DATE COMMENT 'Date portion of created_at.',
    closed_date DATE COMMENT 'Date portion of closed_at.',
    record_hash STRING COMMENT 'Hash propagated from bronze for traceability and change detection.',
    load_timestamp TIMESTAMP COMMENT 'Timestamp when the silver transformation batch was produced.'
)
USING DELTA
PARTITIONED BY (created_year, created_month);

CREATE TABLE IF NOT EXISTS silver.agency_reference (
    agency_code STRING COMMENT 'Agency code from silver.service_requests_clean.',
    agency_name STRING COMMENT 'Agency display name.',
    first_seen_created_date DATE COMMENT 'Earliest created_date seen for the agency in the processed batch.',
    load_timestamp TIMESTAMP COMMENT 'Timestamp when the reference row was produced.'
)
USING DELTA;

CREATE TABLE IF NOT EXISTS silver.complaint_type_reference (
    complaint_type STRING COMMENT 'Complaint type from silver.service_requests_clean.',
    descriptor STRING COMMENT 'Complaint descriptor paired with the complaint type.',
    first_seen_created_date DATE COMMENT 'Earliest created_date seen for the complaint type in the processed batch.',
    load_timestamp TIMESTAMP COMMENT 'Timestamp when the reference row was produced.'
)
USING DELTA;

CREATE TABLE IF NOT EXISTS silver.location_reference (
    incident_zip STRING COMMENT 'Incident ZIP code.',
    borough STRING COMMENT 'Canonical borough value.',
    city STRING COMMENT 'City or locality field.',
    location_type STRING COMMENT 'Location type from the silver request row.',
    latitude DOUBLE COMMENT 'Representative latitude from the processed batch.',
    longitude DOUBLE COMMENT 'Representative longitude from the processed batch.',
    load_timestamp TIMESTAMP COMMENT 'Timestamp when the reference row was produced.'
)
USING DELTA;

CREATE TABLE IF NOT EXISTS silver.status_reference (
    status_name STRING COMMENT 'Normalized request status value.',
    status_group STRING COMMENT 'Starter grouping such as Open or Closed.',
    is_closed_status BOOLEAN COMMENT 'Boolean helper derived from the status meaning.',
    load_timestamp TIMESTAMP COMMENT 'Timestamp when the reference row was produced.'
)
USING DELTA;
