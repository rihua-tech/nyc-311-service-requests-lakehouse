# Medallion Design

## Bronze

- Table: `bronze.nyc311_service_requests_raw`
- Grain: one ingested source record per load event
- Current implementation:
  - the earlier Milestone 9 and 10 Databricks-led runs wrote the bronze Delta table under `abfss://nyc311@<your-storage-account>.dfs.core.windows.net/bronze/nyc311_service_requests_raw/`
  - the current Milestone 11 path adds ADF raw JSON landing and then hands the landed file to `01_ingest_nyc311_raw` with `ingestion_mode=adf_landed_raw`
  - raw JSON payload is stored in `raw_payload`
  - ingest metadata includes `ingest_id`, `source_record_id`, `api_pull_timestamp`, `record_hash`, and `file_path`
  - watermark state is stored under `bronze/checkpoints/nyc311_service_requests/watermark_state/`
  - `file_path` records the landed raw file path in `adf_landed_raw` mode and a lineage-style `raw_batches` path pattern in Databricks-led extraction mode
  - in `adf_landed_raw` mode, bronze does not advance the source watermark because ADF owns extraction for that run

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
  - is materialized by the Databricks silver notebooks in both the earlier Databricks-led proof and the current Milestone 11 handoff path
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
- gold processing runs after the silver quality step and before the separate validation stage in the current Databricks processing flow

## Design Notes

- this is a working starter implementation, not a production warehouse
- the current surrogate-key pattern is intentionally simple and documented in code
- slowly changing dimensions are not implemented
- historical status tracking is not implemented, so backlog snapshot logic is a pragmatic current-state approximation
- validation notebooks are treated as a post-processing check stage rather than a separate medallion layer
