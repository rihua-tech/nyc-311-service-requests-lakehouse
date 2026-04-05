# Pipeline Runbook

## Purpose

Describe the current Milestone 10 execution path for the NYC 311 lakehouse. The project now has a real Databricks workflow/job that runs the pipeline end to end in Databricks, while Azure Data Factory remains future work.

## Current Milestone 10 Execution Path

- the current operating path is a real Databricks workflow/job in Jobs & Pipelines
- the workflow is dependency-driven, so downstream tasks wait for upstream success before they run
- queueing is enabled and maximum concurrent runs is `1`, which keeps overlapping runs from writing at the same time
- manual notebook execution is still useful for debugging, but it is no longer the primary documented run path
- ADF orchestration is still a future phase and should not be described as the active trigger path

## Workflow Parameters

- `environment` selects the runtime configuration for storage, catalog, schemas, and secret expectations
- `run_date` passes a consistent run date through the workflow for date-aware processing and validation logic

## Real Workflow Execution Order

The workflow follows the notebook order below. In the Databricks UI, some sibling tasks may fan out after a shared dependency succeeds, but this is the logical order operators should use when reviewing the run.

1. `01_secrets_and_widgets`
2. `00_mounts_and_paths`
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
18. `01_bronze_validation`
19. `02_silver_validation`
20. `03_gold_validation`

## Pre-Run Checklist

- confirm the job parameters use the intended `environment` and `run_date`
- confirm the target environment config points to the correct storage account, container or filesystem, and Unity Catalog catalog
- confirm the Databricks secret scope and key names exist before the setup tasks start
- confirm the selected compute can start cleanly and has access to the configured catalog and schemas
- confirm no active rerun is already using the same target paths; if another run is already active, let the queue handle sequencing
- decide whether the run is a normal forward run or a replay scenario that should follow the backfill guidance in [docs/runbooks/backfill-runbook.md](../runbooks/backfill-runbook.md)

## Healthy Run Expectations

- the job shows setup, bronze, silver, gold, and validation tasks completing successfully in Jobs & Pipelines
- the setup tasks print resolved environment, storage, and catalog details without exposing secret values
- the ADLS smoke test succeeds
- `bronze.nyc311_service_requests_raw` is created or updated and the watermark state path is refreshed
- `silver.service_requests_clean` plus the silver reference tables are created or updated
- gold dimensions, `gold.fact_service_requests`, and the marts are created or updated
- the validation tasks finish without unresolved null, duplicate, negative-duration, or reconciliation issues

## Retry And Failure Behavior

- the workflow includes basic retry behavior for key setup, bronze, silver, and fact-building tasks to absorb transient startup, storage, or source issues
- dependent tasks do not continue past an upstream failure; repeated failures stop the downstream branch until the root cause is fixed
- this is useful operational hardening, but it should not be described as full production monitoring or full production run-control coverage

## Inspect Failed Tasks In Databricks

1. Open Databricks `Jobs & Pipelines` and select the NYC 311 workflow/job run.
2. Identify the first failed task rather than starting with downstream tasks that were skipped because of dependencies.
3. Open the failed task details and review notebook output, error logs, cluster events, and the Spark UI for that task run.
4. Confirm the run used the intended `environment` and `run_date` values.
5. Fix the earliest failing task first, then rerun the failed path instead of guessing at later-stage symptoms.

## Evidence To Capture

- job parameters screenshot showing `environment` and `run_date`
- successful end-to-end job run screenshot
- workflow DAG screenshots showing setup, bronze, silver, gold, and validation coverage
- task-level proof for setup success, bronze success, silver success, gold success, and validation success when reviewer evidence is needed

Reference screenshots for Milestone 10 live under `docs/screenshots/milestone-10/`, including `m10-job-parameters.png`, `m10-successful-job-run.png`, `m10-workflow-dag-part1-setup-bronze-silver.png`, and `m10-workflow-dag-part2-gold-validation.png`.

## Honest Status

- a real Databricks workflow/job now exists and has successfully run the pipeline end to end for Milestone 10
- the repo still keeps notebook-by-notebook execution available for debugging and targeted reruns
- `infra/adf/` remains the planned Azure Data Factory orchestration path for a later milestone
- production-grade monitoring, alerting, and broader operating controls are still future work
