# Backfill Runbook

## Purpose

Describe how a historical replay or backfill would be handled in the current Databricks-based implementation without pretending there is already a finished production backfill utility. Normal execution now has a real Databricks workflow in Jobs & Pipelines, but replay remains a manual operator process.

## When To Use A Backfill

- a Databricks-based run missed one or more historical source windows
- bronze, silver, or gold logic changed and older dates need reprocessing
- a reviewer wants to understand how replay would be approached in the current implementation

## Core Backfill Principles

- define the historical window before changing any watermark state
- keep replay batches traceable through `run_date`, source window choices, and notebook evidence
- rerun downstream layers only after the historical bronze reload is complete
- validate the replayed window before resuming forward-processing assumptions

## Watermark Considerations

- the current Databricks-based implementation uses `created_date` as the incremental extraction watermark
- capture the current watermark before starting any replay
- use a deliberate `watermark_value` override in Databricks when replaying historical windows
- do not advance the normal watermark permanently until the replayed outputs are accepted

## Suggested Replay Flow

1. Define the backfill window and the downstream tables or marts affected by it.
2. Record the current watermark value before changing anything.
3. Re-run `01_ingest_nyc311_raw` with an intentional historical watermark or bounded extraction choice.
4. Re-run `02_bronze_dedup_metadata`.
5. Re-run the silver notebooks for the same replay scope.
6. Rebuild the affected gold dimensions, fact table, and marts for that replay scope.
7. Run the bronze, silver, and gold validation notebooks for the affected replay scope.
8. Resume the forward watermark only after the replayed outputs are accepted.

## Honest Status

- watermark behavior is implemented in the Databricks bronze path, but replay remains a manual operator process
- the Milestone 10 Databricks workflow covers normal execution, not a separate deployed backfill path
- ADF-driven backfill orchestration and automated replay controls are still future work
