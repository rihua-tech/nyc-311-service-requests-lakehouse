# Medallion Design

## Bronze

- Table: `bronze.nyc311_service_requests_raw`
- Grain: one ingested source record per load event
- Current implementation:
  - raw JSON payload stored in `raw_payload`
  - ingest metadata such as `ingest_id`, `source_record_id`, `api_pull_timestamp`, `record_hash`, and `file_path`
  - local helper logic for bronze record preparation and batch path generation

## Silver

- Table: `silver.service_requests_clean`
- Grain: one standardized service request record
- Current implementation:
  - parses bronze `raw_payload`
  - casts timestamps
  - normalizes borough values
  - derives `created_year`, `created_month`, `created_date`, `closed_date`
  - derives `is_closed`, `is_overdue`, and `resolution_time_hours`
  - removes duplicate `request_id` values
- Supporting tables:
  - `silver.agency_reference`
  - `silver.complaint_type_reference`
  - `silver.location_reference`
  - `silver.status_reference`

## Gold

- Dimensions:
  - `gold.dim_date`
  - `gold.dim_agency`
  - `gold.dim_complaint_type`
  - `gold.dim_location`
  - `gold.dim_status`
- Fact:
  - `gold.fact_service_requests`
- Marts:
  - `gold.mart_request_volume_daily`
  - `gold.mart_service_performance`
  - `gold.mart_backlog_snapshot`

Current implementation:

- gold dimensions are built with starter surrogate-key logic based on stable sorting of business keys
- `gold.fact_service_requests` resolves surrogate keys while retaining selected business columns for readability and downstream compatibility
- gold marts are implemented as fact-based aggregations for request volume, service performance, and backlog snapshots

## Design Notes

- this is still a starter implementation, not a production warehouse
- the current surrogate-key pattern is intentionally simple and documented in code
- slowly changing dimensions are not implemented
- historical status tracking is not implemented, so backlog snapshot logic is a pragmatic current-state approximation
