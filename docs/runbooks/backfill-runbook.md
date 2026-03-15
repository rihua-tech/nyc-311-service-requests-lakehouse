# Backfill Runbook

## Purpose

Outline for historical reloads or replay scenarios.

## Backfill Considerations

- determine the historical date window
- confirm whether bronze should be append-only during replay
- isolate backfill outputs from regular scheduled loads when possible
- validate downstream fact and mart refresh scope

## Suggested Starter Steps

1. Pause scheduled ingestion if overlapping windows would create confusion.
2. Load bounded API windows into a separate raw landing path.
3. Reprocess bronze to silver for the backfill window.
4. Rebuild affected gold tables or partitions.
5. Run reconciliation checks before resuming scheduled loads.

## TODO

- define partition overwrite strategy
- define idempotency behavior for historical loads
- document retention policy for replay files

