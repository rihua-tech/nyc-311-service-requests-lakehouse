# Troubleshooting

Use this checklist when the Milestone 9 manual Databricks-to-ADLS run does not behave as expected. ADF orchestration and deployed jobs are still future work, so start with the notebook path that currently exists.

## Setup Failures

- confirm the Databricks widgets point to the expected `environment`, `catalog`, and secret scope
- confirm the secret scope exists and the configured key names resolve successfully
- confirm the target catalog is visible in `SHOW CATALOGS`
- if ADLS access fails during setup, confirm the storage account and container values match the environment config

## Bronze Failures

- confirm `01_ingest_nyc311_raw` can reach `https://data.cityofnewyork.us/resource/erm2-nwe9.json`
- check whether the current watermark is too restrictive and returning no new rows
- confirm `bronze.nyc311_service_requests_raw` exists before running `02_bronze_dedup_metadata`
- confirm the watermark path under `bronze/checkpoints/nyc311_service_requests/watermark_state/` is writable
- remember that the bronze `file_path` field is lineage metadata; it is not proof of a separate landed raw JSON file

## Silver And Gold Failures

- if a silver notebook fails, confirm the bronze table was created successfully first
- if a gold notebook fails, confirm `silver.service_requests_clean` and the reference tables exist
- identify the first failing notebook in the stage order instead of rerunning everything immediately
- verify the active catalog and schemas are the same across all notebooks in the run

## Validation Failures

- bronze validation failures usually point to missing metadata, empty raw payloads, or duplicate `record_hash` values
- silver validation failures usually point to nulls, duplicate `request_id` values, timestamp parsing problems, unexpected category or borough values, or negative resolution hours
- gold validation failures usually point to missing dimension joins, fact-row mismatches, or mart reconciliation gaps
- fix the earliest failing layer first, then rerun downstream notebooks in order

## Missing Outputs Or Path Confusion

- compare the resolved ADLS paths printed by the setup notebooks with the expected bronze, silver, and gold paths in `infra/azure/storage-structure.md`
- remember that the current Milestone 9 run uses one ADLS filesystem or container, not separate `raw` and `curated` filesystems
- if a reviewer is looking for proof, use the tables, checkpoint paths, and screenshot evidence rather than assuming an ADF raw landing zone already exists

## Pause And Investigate When

- the source API is returning no rows but you expected new data
- checkpoint or watermark state is unclear
- a backfill-style rerun may overlap with the current forward watermark
- downstream gold outputs are missing after an upstream silver failure
