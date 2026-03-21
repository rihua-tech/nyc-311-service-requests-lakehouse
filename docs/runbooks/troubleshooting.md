# Troubleshooting

Use this checklist when the documented Azure flow does not behave as expected. The repo is still scaffolded, so some checks are design-level rather than production-console steps.

## API Or Source Failures

- confirm the source path still points to the NYC 311 dataset `/resource/erm2-nwe9.json`
- verify `run_date`, `window_start`, `window_end`, and `page_size` describe a valid bounded extract
- check for 4xx, 5xx, timeout, or throttling behavior before assuming the issue is downstream
- if an app token is being used, verify the secret or configuration exists and was not hardcoded
- compare the requested source window with the expected daily cadence from `tg_nyc311_daily_ingest`

## Bronze Landing Issues

- confirm the landing path matches `abfss://raw@<your-storage-account>.dfs.core.windows.net/nyc311/service_requests/raw/ingest_date=YYYY-MM-DD/`
- confirm the folder date matches the batch `run_date`
- verify expected file naming such as `nyc311_service_requests_<batch-id>.json`
- check for storage permission or filesystem-name mismatches between `raw`, `curated`, and `logs`
- if a handoff manifest or checkpoint file is referenced, confirm it points under `nyc311/service_requests/checkpoints/`

## Databricks Notebook Or Job Failures

- identify the exact failing task in `wf_nyc311_lakehouse` instead of treating the whole run as one undifferentiated failure
- confirm the shared parameters are aligned: `environment`, `run_date`, `raw_landing_path`, `checkpoint_path`, `catalog`, and schema names
- verify the cluster placeholder assumptions still make sense for the run: runtime, node types, policy, and access mode
- confirm the `00_setup` notebooks or equivalent environment setup has been handled before expecting the workflow to run cleanly
- if a silver or gold notebook fails, check whether the upstream notebook actually produced the table or reference output it expects

## Validation Failures

- determine whether the failure is isolated to bronze, silver, or gold before rerunning everything
- bronze validation failures usually point to missing raw payloads, missing metadata, or duplicate `record_hash` issues
- silver validation failures usually point to nulls, duplicate `request_id` values, timestamp parsing problems, or unexpected normalized category values
- gold validation failures usually point to missing upstream reference outputs, dimension or fact dependency issues, or mart reconciliation mismatches
- fix the earliest failing layer first, then rerun downstream stages in order

## Missing Outputs Or Path Mismatches

- compare ADF raw landing paths, Databricks shared parameters, and storage docs side by side
- keep `run_date`, `ingest_date`, and `load_date` usage consistent; mismatched folder semantics are a common source of missing-output confusion
- confirm bronze lands in the `raw` filesystem while silver and gold are documented under `curated`
- check whether the reviewer is looking for raw JSON files, curated tables, or validation summaries; they live in different places
- if outputs exist but appear stale, verify whether the trigger is only documented or actually active in the target environment

## Pause And Investigate When

- source windows overlap unexpectedly
- checkpoint or watermark state is unclear
- a backfill run and a normal scheduled run are competing for the same dates
- downstream gold outputs are missing after an upstream silver failure
