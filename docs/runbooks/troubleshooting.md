# Troubleshooting

Use this checklist when the Milestone 11 ADF raw landing and Databricks handoff path does not behave as expected. Earlier Milestone 10 workflow evidence remains valid, but the current documented operating path starts in ADF. Fix the earliest failing step first.

## ADF And Handoff Checks

- open the ADF pipeline run and identify whether the failure happened during raw landing or during the Databricks handoff
- inspect the ADF activity output, then inspect Databricks notebook output, error logs, cluster events, and Spark UI before rerunning anything
- confirm the run used the intended `environment`, `catalog`, `run_date`, `window_start`, `window_end`, and `batch_id` parameters
- confirm the handoff passed `ingestion_mode=adf_landed_raw` and the expected `raw_landing_path`
- confirm the selected compute is healthy and did not fail on startup, library resolution, or workspace access
- confirm the linked services `ls_nyc311_http`, `ls_adls_nyc311`, and `ls_databricks_nyc311` point at the intended source, storage, and workspace
- if the repo starter name differs from the linked service used in the final proof, confirm the active ADF Databricks linked service points to the intended workspace before rerunning

## Setup Failures

- confirm `01_secrets_and_widgets` resolves the expected `environment`, `catalog`, and secret scope values
- confirm the secret scope exists and the configured key names resolve successfully
- confirm the target catalog is visible in `SHOW CATALOGS`
- if `00_mounts_and_paths` fails, confirm the storage account and container or filesystem values match the environment config
- if setup fails, expect every downstream task to stop because the workflow dependencies are working as designed

## Bronze Failures

- confirm ADF landed a file under `abfss://raw@<storage-account>.dfs.core.windows.net/nyc311/service_requests/raw/ingest_date=<run_date>/`
- confirm `01_ingest_nyc311_raw` received `ingestion_mode=adf_landed_raw` and the matching `raw_landing_path`
- confirm `bronze.nyc311_service_requests_raw` exists before `02_bronze_dedup_metadata` runs
- confirm the watermark path under `bronze/checkpoints/nyc311_service_requests/watermark_state/` is writable
- remember that the landed raw JSON file is now real, while the bronze `file_path` field remains lineage metadata after the handoff read
- in `adf_landed_raw` mode the source watermark is not advanced by the bronze notebook because ADF owns the extraction window
- if basic task retries were already attempted and the task still failed, inspect the task logs before starting another full rerun

## Silver Failures

- confirm the bronze tasks completed successfully before troubleshooting any silver task
- if `01_clean_service_requests` fails, start with schema, null handling, timestamp parsing, and source-column assumptions
- if `02_standardize_locations` or `03_standardize_categories` fails, confirm `silver.service_requests_clean` was published successfully first
- if `04_apply_quality_rules` fails, inspect the upstream location and category outputs before changing the quality checks
- verify the active catalog and schemas are the same across all tasks in the run

## Gold Failures

- confirm `04_apply_quality_rules` succeeded before troubleshooting gold tasks
- if a dimension task fails, inspect the silver outputs it depends on before rerunning the full job
- if `06_build_fact_service_requests` fails, confirm all required dimensions were built successfully first
- if a mart task fails, confirm `gold.fact_service_requests` exists and the upstream fact load completed cleanly
- if backlog snapshot or gold validation results look wrong, double-check the `run_date` passed into the handoff and downstream run because date-aware logic uses it when no explicit snapshot date is provided

## Validation Failures

- bronze validation failures usually point to missing metadata, empty raw payloads, or duplicate `record_hash` values
- silver validation failures usually point to nulls, duplicate `request_id` values, timestamp parsing problems, unexpected category or borough values, or negative resolution hours
- gold validation failures usually point to missing dimension joins, fact-row mismatches, or mart reconciliation gaps
- in the Milestone 11 workspace, the legacy validation notebooks may fail or be less useful when they expect registered metastore tables in `spark_catalog`
- if that happens, validate the ADLS-backed Delta paths directly and compare counts or outputs manually
- fix the earliest failing layer first, then rerun the failed path in order

## Missing Outputs Or Path Confusion

- compare the resolved ADLS paths printed by the setup tasks with the expected bronze, silver, and gold paths in [infra/azure/storage-structure.md](../../infra/azure/storage-structure.md)
- remember that Milestone 11 now does depend on a real ADF raw landing pattern before the Databricks handoff
- if a reviewer is looking for proof, use the landed raw file, Delta paths, handoff evidence, and Milestone 11 screenshots rather than only the older workflow evidence

## Workspace-Specific Issues

- if the original workspace shows a stale credits-exhausted state after the subscription upgrade, do not treat that as proof that the repo logic failed
- use the new workspace `dbw-test-centralus-01` for the Milestone 11 proof path and keep the earlier Milestone 9 and 10 screenshots as valid historical evidence
- if validation notebooks behave differently across workspaces, check whether the workspace expects registered tables in `spark_catalog` before assuming the Delta outputs are wrong

## Pause And Investigate When

- the ADF raw landing file is missing or empty
- the run parameters look wrong for the intended environment or date
- checkpoint or watermark state is unclear
- compute is unstable or repeatedly restarting
- a backfill-style rerun may overlap with the current forward watermark
- downstream gold outputs are missing after an upstream bronze or silver failure
