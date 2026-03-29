# Pipeline Runbook

## Purpose

Describe the current Milestone 9 manual cloud run for the NYC 311 lakehouse and separate it from the future orchestration path that is still scaffolded.

## Current Milestone 9 Run

1. Run `databricks/notebooks/00_setup/01_secrets_and_widgets` to validate widgets, secret names, and catalog visibility.
2. Run `databricks/notebooks/00_setup/00_mounts_and_paths` to confirm ADLS access, base paths, and a small storage smoke test.
3. Run `databricks/notebooks/01_bronze/01_ingest_nyc311_raw` to call the NYC 311 API directly from Databricks, create bronze-ready rows, write `bronze.nyc311_service_requests_raw`, and update the watermark state.
4. Run `databricks/notebooks/01_bronze/02_bronze_dedup_metadata` to deduplicate bronze rows and republish the bronze table.
5. Run the silver notebooks in order:
   - `databricks/notebooks/02_silver/01_clean_service_requests`
   - `databricks/notebooks/02_silver/02_standardize_locations`
   - `databricks/notebooks/02_silver/03_standardize_categories`
   - `databricks/notebooks/02_silver/04_apply_quality_rules`
6. Run the gold notebooks in order:
   - `databricks/notebooks/03_gold/01_build_dim_date`
   - `databricks/notebooks/03_gold/02_build_dim_agency`
   - `databricks/notebooks/03_gold/03_build_dim_complaint_type`
   - `databricks/notebooks/03_gold/04_build_dim_location`
   - `databricks/notebooks/03_gold/05_build_dim_status`
   - `databricks/notebooks/03_gold/06_build_fact_service_requests`
   - `databricks/notebooks/03_gold/07_build_mart_request_volume_daily`
   - `databricks/notebooks/03_gold/08_build_mart_service_performance`
   - `databricks/notebooks/03_gold/09_build_mart_backlog_snapshot`
7. Run the validation notebooks in order:
   - `databricks/notebooks/04_validation/01_bronze_validation`
   - `databricks/notebooks/04_validation/02_silver_validation`
   - `databricks/notebooks/04_validation/03_gold_validation`

## Pre-Run Checklist

- confirm the target environment config points to the correct storage account, container, and Unity Catalog catalog
- confirm the Databricks secret scope and key names exist before running the setup notebooks
- confirm the current user or cluster can use the configured catalog and create or update the bronze, silver, and gold schemas
- confirm no overlapping manual rerun is already writing to the same target paths
- decide whether the run should use the existing watermark or a one-off `watermark_value` override

## Healthy Run Expectations

- the setup notebooks print the resolved environment, storage, and catalog details without exposing secret values
- the ADLS smoke test succeeds
- `bronze.nyc311_service_requests_raw` is created or updated and the watermark state path is refreshed
- `silver.service_requests_clean` plus the silver reference tables are created or updated
- gold dimensions, `gold.fact_service_requests`, and the marts are created or updated
- the validation notebooks finish without unresolved null, duplicate, negative-duration, or reconciliation issues

## Evidence To Capture

- Databricks compute running
- secret scope validation success
- ADLS smoke-test success
- bronze ingest and bronze dedup success
- silver clean, reference, and quality-rule success
- gold dimension, fact, and mart success
- bronze, silver, and gold validation success

Reference screenshots live under `docs/screenshots/milestone-9/`.

## Future Orchestration Path

- `infra/adf/` still represents the planned Azure Data Factory orchestration path
- `infra/databricks/workflow-job.json` still represents the planned Databricks job definition
- those assets are useful deployment notes, but they are not the mechanism used for the current Milestone 9 manual run

## Honest Status

- manual Databricks execution against ADLS is working and documented in this repo
- ADF-triggered orchestration, deployed Databricks jobs, and production operating controls are still future work
