CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.nyc311_service_requests_raw (
    ingest_id STRING,
    source_system STRING,
    source_record_id STRING,
    ingest_timestamp TIMESTAMP,
    ingest_date DATE,
    api_pull_timestamp TIMESTAMP,
    raw_payload STRING,
    record_hash STRING,
    file_path STRING
)
USING DELTA;

-- TODO: Add partitioning and table properties only after ingestion patterns are confirmed.

