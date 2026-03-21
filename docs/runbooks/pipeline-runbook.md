# Pipeline Runbook

## Purpose

Describe the intended normal Azure run path for the NYC 311 lakehouse. This is an operating guide for the current scaffolds, not evidence of a completed deployment.

## Intended Normal Run

1. NYC 311 source data is requested through ADF pipeline `pl_nyc311_ingest`, either by a manual run or by the placeholder trigger `tg_nyc311_daily_ingest`.
2. ADF activity `CopyNYC311ToBronzeRaw` pulls a bounded extract from `/resource/erm2-nwe9.json` using `run_date`, `window_start`, `window_end`, and `page_size`.
3. ADF lands the raw response in ADLS under `abfss://raw@<your-storage-account>.dfs.core.windows.net/nyc311/service_requests/raw/ingest_date=YYYY-MM-DD/`.
4. ADF then hands control to a Databricks notebook placeholder that passes the raw landing context and points toward workflow `wf_nyc311_lakehouse`.
5. Databricks runs bronze processing in sequence:
   - `databricks/notebooks/01_bronze/01_ingest_nyc311_raw`
   - `databricks/notebooks/01_bronze/02_bronze_dedup_metadata`
6. Databricks runs silver processing in sequence:
   - `databricks/notebooks/02_silver/01_clean_service_requests`
   - `databricks/notebooks/02_silver/02_standardize_locations`
   - `databricks/notebooks/02_silver/03_standardize_categories`
   - `databricks/notebooks/02_silver/04_apply_quality_rules`
7. Databricks runs gold processing in sequence:
   - `databricks/notebooks/03_gold/01_build_dim_date`
   - `databricks/notebooks/03_gold/02_build_dim_agency`
   - `databricks/notebooks/03_gold/03_build_dim_complaint_type`
   - `databricks/notebooks/03_gold/04_build_dim_location`
   - `databricks/notebooks/03_gold/05_build_dim_status`
   - `databricks/notebooks/03_gold/06_build_fact_service_requests`
   - `databricks/notebooks/03_gold/07_build_mart_request_volume_daily`
   - `databricks/notebooks/03_gold/08_build_mart_service_performance`
   - `databricks/notebooks/03_gold/09_build_mart_backlog_snapshot`
8. Databricks runs post-processing validation notebooks:
   - `databricks/notebooks/04_validation/01_bronze_validation`
   - `databricks/notebooks/04_validation/02_silver_validation`
   - `databricks/notebooks/04_validation/03_gold_validation`

## Pre-Run Checklist

- placeholder resource names replaced for the target environment
- raw and curated storage paths confirmed against `infra/azure/storage-structure.md`
- Databricks cluster and secret assumptions reviewed against `infra/databricks/cluster-config.json` and `infra/databricks/secret-scope-notes.md`
- source window parameters reviewed so `run_date`, `window_start`, and `window_end` describe the same batch
- overlapping manual and scheduled runs avoided

## Healthy Run Expectations

- ADF copy completes and writes a dated raw file with the expected batch identifier.
- Databricks advances from bronze to silver to gold to validation in the documented order.
- Bronze data is available in `bronze.nyc311_service_requests_raw`.
- Silver outputs are available in `silver.service_requests_clean` plus the supporting reference tables.
- Gold dimensions, fact table, and marts are refreshed for the requested window.
- Validation notebooks finish without unresolved row-count, null, duplicate, or reconciliation concerns.

## Honest Status

- the repo documents the intended Azure orchestration path, but the runtime wiring is still scaffolded
- the local Python modules remain the most concrete implementation today
- this runbook is a design-level operating guide, not a production SOP
