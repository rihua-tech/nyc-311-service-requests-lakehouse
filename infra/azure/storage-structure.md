# Storage Structure

This document describes the current Milestone 9 ADLS layout used by the manual Databricks cloud run. It also calls out where the repo still contains future-state placeholders.

## Current ADLS Layout

The current dev configuration uses a single ADLS Gen2 filesystem or container named `nyc311`. Bronze, silver, gold, and checkpoint paths are all resolved under that filesystem.

| Area | Example path | Current use |
| --- | --- | --- |
| Bronze table storage | `abfss://nyc311@<your-storage-account>.dfs.core.windows.net/bronze/nyc311_service_requests_raw/` | Delta location for `bronze.nyc311_service_requests_raw` |
| Bronze lineage path pattern | `abfss://nyc311@<your-storage-account>.dfs.core.windows.net/bronze/nyc311_service_requests_raw/raw_batches/ingest_date=YYYY-MM-DD/batch_<uuid>.json` | stored in the bronze `file_path` column for lineage metadata |
| Bronze checkpoints | `abfss://nyc311@<your-storage-account>.dfs.core.windows.net/bronze/checkpoints/` | base checkpoint area for setup smoke tests and watermark-related state |
| Watermark state | `abfss://nyc311@<your-storage-account>.dfs.core.windows.net/bronze/checkpoints/nyc311_service_requests/watermark_state/` | latest incremental extraction watermark |
| Silver outputs | `abfss://nyc311@<your-storage-account>.dfs.core.windows.net/silver/<table-name>/` | Delta locations for `silver.service_requests_clean` and silver reference tables |
| Gold outputs | `abfss://nyc311@<your-storage-account>.dfs.core.windows.net/gold/<table-name>/` | Delta locations for dimensions, fact table, and marts |

## High-Level Folder Shape

```text
nyc311/
  bronze/
    nyc311_service_requests_raw/
    checkpoints/
      _setup_storage_smoke_test/
      nyc311_service_requests/
        watermark_state/
  silver/
    service_requests_clean/
    agency_reference/
    complaint_type_reference/
    location_reference/
    status_reference/
  gold/
    dim_date/
    dim_agency/
    dim_complaint_type/
    dim_location/
    dim_status/
    fact_service_requests/
    mart_request_volume_daily/
    mart_service_performance/
    mart_backlog_snapshot/
```

## Important Notes

- the current Milestone 9 run writes Delta tables and watermark state to ADLS
- the `raw_batches` pattern is currently carried as bronze lineage metadata; the repo does not yet implement a separate raw JSON landing flow in ADLS before bronze table creation
- this differs from the earlier scaffold that assumed separate `raw`, `curated`, and `logs` filesystems
- if ADF-led raw landing is implemented later, the storage layout can expand without changing the current bronze, silver, and gold Delta table locations

## Future-State Placeholder Notes

- `infra/adf/` still models a future ingestion handoff path into storage and Databricks
- dedicated log containers, separate raw and curated filesystems, and stricter environment isolation are not yet implemented in this repo
