# Storage Structure

This document summarizes the shared-filesystem ADLS layout used in the earlier Milestone 9 and 10 Databricks cloud runs. It also notes how the current Milestone 11 proof adds ADF-led raw landing ahead of the Databricks bronze step.

## Shared-Filesystem Dev Layout

The earlier cloud runs used a single ADLS Gen2 filesystem or container named `nyc311`. Bronze, silver, gold, and checkpoint paths were all resolved under that filesystem.

| Area | Example path | Current use |
| --- | --- | --- |
| Bronze table storage | `abfss://nyc311@<your-storage-account>.dfs.core.windows.net/bronze/nyc311_service_requests_raw/` | Delta location for `bronze.nyc311_service_requests_raw` |
| Bronze lineage path pattern | `abfss://nyc311@<your-storage-account>.dfs.core.windows.net/bronze/nyc311_service_requests_raw/raw_batches/ingest_date=YYYY-MM-DD/batch_<uuid>.json` | stored in the bronze `file_path` column for lineage metadata |
| Bronze checkpoints | `abfss://nyc311@<your-storage-account>.dfs.core.windows.net/bronze/checkpoints/` | base checkpoint area for setup smoke tests and watermark-related state |
| Watermark state | `abfss://nyc311@<your-storage-account>.dfs.core.windows.net/bronze/checkpoints/nyc311_service_requests/watermark_state/` | latest incremental extraction watermark |
| Silver outputs | `abfss://nyc311@<your-storage-account>.dfs.core.windows.net/silver/<table-name>/` | Delta locations for `silver.service_requests_clean` and silver reference tables |
| Gold outputs | `abfss://nyc311@<your-storage-account>.dfs.core.windows.net/gold/<table-name>/` | Delta locations for dimensions, fact table, and marts |

## Milestone 11 Raw Landing Addition

The current proven Milestone 11 path adds ADF-led raw JSON landing before Databricks bronze processing. In that path, ADF lands a file such as:

`abfss://raw@<your-storage-account>.dfs.core.windows.net/nyc311/raw/service_requests/ingest_date=YYYY-MM-DD/nyc311_service_requests_<batch_id>.json`

Databricks then receives that landed file path through `raw_landing_path` when `ingestion_mode=adf_landed_raw`.

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

- the earlier Milestone 9 and 10 proof writes Delta tables and watermark state to ADLS from Databricks
- the `raw_batches` pattern remains useful bronze lineage metadata for Databricks-led extraction runs
- the current Milestone 11 proof separately demonstrates ADF raw landing before the bronze notebook handoff
- dedicated `raw`, `curated`, and `logs` filesystems remain starter documentation rather than a fully implemented storage layout in this repo

## Future-State Placeholder Notes

- dedicated log containers, separate raw and curated filesystems, and stricter environment isolation are not yet implemented in this repo
