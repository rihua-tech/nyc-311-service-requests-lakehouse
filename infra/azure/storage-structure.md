# Storage Structure

Intended ADLS Gen2 layout for the NYC 311 lakehouse. This is a placeholder design reference only; it is not a deployed storage definition.

## Zone Map

| Zone | Filesystem | Example path | Purpose |
| --- | --- | --- | --- |
| Bronze | `raw` | `abfss://raw@<your-storage-account>.dfs.core.windows.net/nyc311/service_requests/raw/ingest_date=YYYY-MM-DD/` | Immutable raw NYC 311 API payloads landed by ADF before Databricks processing. |
| Silver | `curated` | `abfss://curated@<your-storage-account>.dfs.core.windows.net/silver/service_requests_clean/load_date=YYYY-MM-DD/` | Cleaned Databricks outputs and silver reference datasets. |
| Gold | `curated` | `abfss://curated@<your-storage-account>.dfs.core.windows.net/gold/fact_service_requests/load_date=YYYY-MM-DD/` | Gold dimensions, fact table, and reporting marts for downstream analytics. |
| Checkpoints | `raw` | `abfss://raw@<your-storage-account>.dfs.core.windows.net/nyc311/service_requests/checkpoints/` | Watermarks, ADF handoff metadata, and replay or restart state. |
| Logs | `logs` | `abfss://logs@<your-storage-account>.dfs.core.windows.net/adf/pl_nyc311_ingest/run_date=YYYY-MM-DD/` | Operational logs and audit artifacts from ADF and Databricks runs. |

## Suggested Layout

```text
raw/
  nyc311/
    service_requests/
      raw/
        ingest_date=YYYY-MM-DD/
          nyc311_service_requests_<batch-id>.json
      checkpoints/
        api_watermark/
        adf_handoff/
          run_date=YYYY-MM-DD/

curated/
  silver/
    service_requests_clean/
      load_date=YYYY-MM-DD/
    reference/
      agency/
      complaint_type/
      location/
      status/
  gold/
    dim_date/
    dim_agency/
    dim_complaint_type/
    dim_location/
    dim_status/
    fact_service_requests/
      load_date=YYYY-MM-DD/
    marts/
      mart_request_volume_daily/
      mart_service_performance/
      mart_backlog_snapshot/

logs/
  adf/
    pl_nyc311_ingest/
      run_date=YYYY-MM-DD/
  databricks/
    wf_nyc311_lakehouse/
      run_date=YYYY-MM-DD/
```

## Notes

- Treat the `raw` filesystem as append-oriented landing storage for bronze inputs and checkpoint artifacts.
- Keep checkpoint state separate from curated silver and gold data so replays remain simpler to reason about.
- Store silver and gold outputs in the `curated` filesystem even though their logical ownership is still the Databricks medallion layers.
- Use environment-specific storage accounts, file systems, and access policies when these placeholders are replaced with real infrastructure.
