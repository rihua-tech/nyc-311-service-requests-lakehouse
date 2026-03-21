# Backfill Runbook

## Purpose

Describe how a historical reload or replay would work conceptually once the Azure path is wired. This runbook is intentionally honest about the fact that the runtime wiring is still scaffolded.

## When To Use A Backfill

- a scheduled run missed one or more historical source windows
- bronze, silver, or gold logic changed and older dates need reprocessing
- a reviewer wants to understand how replay would be handled without assuming the deployment already exists

## Core Backfill Principles

- define the historical window before changing any watermark or checkpoint state
- separate backfill batches from normal daily batches whenever overlapping dates could create confusion
- keep replay logic traceable through `run_date`, `batch_id`, and landing-path conventions
- rerun downstream layers only after the historical raw landing is complete
- validate the replayed window before resuming normal forward-processing assumptions

## Watermark And Checkpoint Considerations

- the repo currently uses `created_date` as the conceptual source watermark for incremental extraction
- capture the current forward watermark before starting any backfill window
- do not advance the normal production watermark until the historical replay has been validated
- if the replay uses checkpoint files, prefer a separate backfill checkpoint location or clearly separated batch identifiers under `nyc311/service_requests/checkpoints/`
- decide up front whether the backfill is append-only, replace-by-window, or a side-by-side replay; that choice affects how bronze, silver, and gold outputs should be interpreted

## Suggested Conceptual Flow

1. Define the backfill window and the downstream tables or marts affected by it.
2. Pause or suppress the regular trigger if the same dates might be processed twice.
3. Run ADF with explicit `run_date`, `window_start`, `window_end`, and `batch_id` values for the historical window.
4. Land historical raw files in the standard bronze pattern or in an isolated replay path that still preserves `ingest_date` traceability.
5. Re-run Databricks bronze processing for the historical raw files.
6. Re-run the silver notebooks for the same historical scope.
7. Rebuild the affected gold dimensions, fact table, and marts for the backfill window.
8. Run bronze, silver, and gold validation checks before resuming normal scheduled loads.
9. Resume the forward watermark only after the replayed outputs are accepted.

## Practical Notes

- daily or weekly historical windows are easier to reason about than one large replay request
- distinct backfill batch identifiers make troubleshooting and audit review simpler
- if partition overwrite behavior is not fully defined yet, a clearly scoped rebuild is safer than ad hoc table edits
- keep backfill logs and reviewer notes with the same run metadata used in the replay

## Honest Status

- checkpoint and watermark behavior is documented conceptually in this repo and is not yet enforced by a live Azure deployment
- this runbook explains intended replay handling, not a finished production backfill utility
