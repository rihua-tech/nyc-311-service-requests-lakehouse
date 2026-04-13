# Pipeline Runbook

## Purpose

Describe the current Milestone 11 execution path for the NYC 311 lakehouse. The current documented path is ADF raw landing plus a Databricks notebook handoff, while earlier Milestone 9 and 10 Databricks notebook and workflow evidence remains valid historical proof.

## Current Milestone 11 Execution Path

- the current operating path starts in ADF with a real REST extraction and ADLS raw landing
- ADF then triggers the Databricks handoff notebook `01_ingest_nyc311_raw` with `ingestion_mode=adf_landed_raw`
- the earlier Databricks workflow/job evidence from Milestone 10 remains valid, but it is not the only cloud proof in the repo anymore
- the original workspace later entered a stale credits-exhausted state after a subscription upgrade, so the final Milestone 11 proof was captured in `dbw-test-centralus-01`
- manual notebook execution is still useful for debugging and for replaying downstream stages after the handoff

## ADF Pipeline Parameters

- `environment` selects the runtime configuration for storage, catalog, schemas, and secret expectations
- `catalog` selects the target catalog for the Databricks notebook session; Milestone 11 compatibility work used `spark_catalog`
- `run_date` passes a consistent run date through the workflow for date-aware processing and validation logic
- `window_start` and `window_end` define the bounded extraction window that ADF owns for the source pull
- `page_size` sets the API page size used by the copy activity
- `batch_id` provides the run identifier used in the landed filename and downstream lineage

## Notebook Handoff Mapping

- `environment` -> `environment`
- `catalog` -> `catalog`
- `run_date` -> `run_date`
- `batch_id` -> `batch_id`
- `window_start` -> `window_start`
- `window_end` -> `window_end`
- constant `adf_landed_raw` -> `ingestion_mode`
- derived `abfss://raw@<storage-account>.dfs.core.windows.net/nyc311/service_requests/raw/ingest_date=<run_date>/nyc311_service_requests_<batch_id>.json` -> `raw_landing_path`

## Real Execution Order

The Milestone 11 proof path follows the sequence below.

1. ADF `CopyNYC311ToBronzeRaw`
2. ADF Databricks notebook activity using `ls_databricks_nyc311`
3. `01_ingest_nyc311_raw`
4. `02_bronze_dedup_metadata`
5. `01_clean_service_requests`
6. `02_standardize_locations`
7. `03_standardize_categories`
8. `04_apply_quality_rules`
9. `01_build_dim_date`
10. `02_build_dim_agency`
11. `03_build_dim_complaint_type`
12. `04_build_dim_location`
13. `05_build_dim_status`
14. `06_build_fact_service_requests`
15. `07_build_mart_request_volume_daily`
16. `08_build_mart_service_performance`
17. `09_build_mart_backlog_snapshot`
18. manual or path-based validation against the ADLS-backed Delta outputs

## Pre-Run Checklist

- confirm the ADF run parameters use the intended `environment`, `catalog`, `run_date`, `window_start`, `window_end`, `page_size`, and `batch_id`
- confirm the target environment config points to the correct storage account, container or filesystem, and catalog
- confirm the Databricks secret scope and key names exist before the setup tasks start
- confirm the selected compute can start cleanly and has access to the configured catalog and schemas
- confirm the linked services `ls_nyc311_http`, `ls_adls_nyc311`, and the active Databricks linked service point at the intended source, storage account, and workspace
- note that the repo-side starter JSON uses `ls_databricks_nyc311`, while the final Milestone 11 proof used a fresh linked service pointing to `dbw-test-centralus-01`
- confirm no active rerun is already using the same target paths
- decide whether the run is a normal forward run or a replay scenario that should follow the backfill guidance in [docs/runbooks/backfill-runbook.md](../runbooks/backfill-runbook.md)

## Healthy Run Expectations

- ADF lands a file at `abfss://raw@<storage-account>.dfs.core.windows.net/nyc311/service_requests/raw/ingest_date=<run_date>/nyc311_service_requests_<batch_id>.json`
- the Databricks handoff receives `ingestion_mode=adf_landed_raw` and the expected `raw_landing_path`
- the setup tasks print resolved environment, storage, and catalog details without exposing secret values
- the ADLS smoke test succeeds
- the bronze Delta output or registered bronze path is created or updated from the landed raw JSON
- when `ingestion_mode=adf_landed_raw`, the source watermark is not advanced by `01_ingest_nyc311_raw` because ADF owns the extraction window
- `silver.service_requests_clean` plus the silver reference tables are created or updated
- gold dimensions, `gold.fact_service_requests`, and the marts are created or updated
- final Milestone 11 validation is completed by manually checking the Delta paths and expected counts or outputs in the new workspace

## Manual Verification Steps

1. Confirm the ADF pipeline run succeeded with the intended `run_date`, `window_start`, `window_end`, and `batch_id`.
2. Confirm the landed raw JSON exists under `abfss://raw@<storage-account>.dfs.core.windows.net/nyc311/service_requests/raw/ingest_date=<run_date>/`.
3. Confirm the Databricks handoff notebook used `ingestion_mode=adf_landed_raw`, the matching `raw_landing_path`, and the intended `catalog`.
4. Confirm the bronze Delta path was updated and the landed batch can be traced through the bronze `file_path` or metadata fields.
5. Confirm silver and gold Delta paths were updated.
6. In the Milestone 11 workspace, validate the resulting ADLS-backed Delta data directly when the legacy validation notebooks expect registered `spark_catalog` tables.

## Inspect Failed Tasks In ADF Or Databricks

1. Open the ADF pipeline run first and confirm whether the failure occurred during raw landing or during the Databricks handoff.
2. If the ADF copy succeeded, inspect the Databricks notebook output for `01_ingest_nyc311_raw`.
3. Review notebook output, error logs, cluster events, and the Spark UI for the earliest failing Databricks step.
4. Confirm the run used the intended `environment`, `catalog`, `run_date`, and landed raw path.
5. Fix the earliest failing step first, then rerun the failed path instead of guessing at later-stage symptoms.

## Evidence To Capture

- ADF pipeline canvas and successful ADF run screenshots
- proof of the raw landing file in ADLS
- Databricks handoff and bronze receipt screenshots
- task-level proof for silver, gold, and final validation checks when reviewer evidence is needed

Reference screenshots for Milestone 11 live under `docs/screenshots/milestone-11/`, including `m11_adf_pipeline_canvas.png`, `m11_adf_run_success.png`, `m11_adls_raw_landing.png`, `m11_adf_to_databricks_handoff.png`, `m11_databricks_run_success.png`, `m11_bronze_received_batch.png`, `m11_end_to_end_architecture.png`, `m11_gold_outputs.png`, and `m11_validation_passed.png`.

## Honest Status

- a real Databricks workflow/job exists and its Milestone 10 evidence remains valid
- Milestone 11 adds a real ADF raw landing and Databricks notebook handoff
- the repo still keeps notebook-by-notebook execution available for debugging and targeted reruns
- the final Milestone 11 validation in `dbw-test-centralus-01` was manual or path-based because the legacy validation notebooks expected registered `spark_catalog` tables
- production-grade monitoring, alerting, and broader operating controls are still future work
