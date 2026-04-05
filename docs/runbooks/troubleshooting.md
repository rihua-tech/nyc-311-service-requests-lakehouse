# Troubleshooting

Use this checklist when the Milestone 10 Databricks workflow/job does not behave as expected. The current operating path is the real Databricks workflow in Jobs & Pipelines, while ADF orchestration is still future work. Fix the earliest failing task first.

## Workflow-First Checks

- open `Jobs & Pipelines`, select the failed run, and identify the first failed task rather than the downstream tasks that were skipped
- inspect the failed task details, notebook output, error logs, cluster events, and Spark UI before rerunning anything
- confirm the run used the intended `environment` and `run_date` parameters
- confirm the workflow dependencies line up with the expected notebook order before assuming a later-stage data issue
- confirm the selected compute is healthy and did not fail on startup, library resolution, or workspace access
- if a new run is waiting, remember the workflow is intentionally queued and limited to one concurrent run

## Setup Failures

- confirm `01_secrets_and_widgets` resolves the expected `environment`, `catalog`, and secret scope values
- confirm the secret scope exists and the configured key names resolve successfully
- confirm the target catalog is visible in `SHOW CATALOGS`
- if `00_mounts_and_paths` fails, confirm the storage account and container or filesystem values match the environment config
- if setup fails, expect every downstream task to stop because the workflow dependencies are working as designed

## Bronze Failures

- confirm `01_ingest_nyc311_raw` can reach `https://data.cityofnewyork.us/resource/erm2-nwe9.json`
- check whether the current watermark is too restrictive and returning no new rows
- confirm `bronze.nyc311_service_requests_raw` exists before `02_bronze_dedup_metadata` runs
- confirm the watermark path under `bronze/checkpoints/nyc311_service_requests/watermark_state/` is writable
- remember that the bronze `file_path` field is lineage metadata; it is not proof of a separate landed raw JSON file
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
- if backlog snapshot or gold validation results look wrong, double-check the `run_date` passed into the workflow because date-aware logic uses it when no explicit snapshot date is provided

## Validation Failures

- bronze validation failures usually point to missing metadata, empty raw payloads, or duplicate `record_hash` values
- silver validation failures usually point to nulls, duplicate `request_id` values, timestamp parsing problems, unexpected category or borough values, or negative resolution hours
- gold validation failures usually point to missing dimension joins, fact-row mismatches, or mart reconciliation gaps
- fix the earliest failing layer first, then rerun the failed path in order

## Missing Outputs Or Path Confusion

- compare the resolved ADLS paths printed by the setup tasks with the expected bronze, silver, and gold paths in [infra/azure/storage-structure.md](../../infra/azure/storage-structure.md)
- remember that the current implementation uses the configured ADLS filesystem or container for bronze, silver, and gold outputs; it does not depend on an already-live ADF raw landing pattern
- if a reviewer is looking for proof, use the tables, checkpoint paths, workflow run details, and Milestone 10 screenshot evidence rather than assuming an ADF landing zone already exists

## Pause And Investigate When

- the source API is returning no rows but you expected new data
- the run parameters look wrong for the intended environment or date
- checkpoint or watermark state is unclear
- compute is unstable or repeatedly restarting
- a backfill-style rerun may overlap with the current forward watermark
- downstream gold outputs are missing after an upstream bronze or silver failure
