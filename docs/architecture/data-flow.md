# Data Flow

## Current Milestone 11 Execution Path

| Step | Source | Target | Purpose |
| --- | --- | --- | --- |
| 1 | ADF `CopyNYC311ToBronzeRaw` | raw ADLS landing path | call the NYC 311 REST API and land bounded raw JSON to ADLS |
| 2 | ADF Databricks notebook activity | `01_ingest_nyc311_raw` | pass the handoff parameters including `ingestion_mode=adf_landed_raw` and the landed raw path |
| 3 | `01_ingest_nyc311_raw` | `bronze.nyc311_service_requests_raw` | read the landed raw JSON, attach ingest metadata, and write the bronze Delta table |
| 4 | `02_bronze_dedup_metadata` | `bronze.nyc311_service_requests_raw` | remove duplicate bronze records and republish the bronze table |
| 5 | silver notebooks | `silver.service_requests_clean` and silver reference tables | clean request fields, standardize locations and categories, and run silver quality rules |
| 6 | gold notebooks | gold dimensions, fact table, and marts | build reporting-ready gold outputs |
| 7 | manual or path-based validation | ADLS-backed Delta outputs | confirm completeness and expected outputs in the Milestone 11 workspace where the legacy validation notebooks expected registered `spark_catalog` tables |
| 8 | gold outputs | future Power BI or SQL consumers | expose reporting-friendly datasets for downstream analysis |

## ADF Parameters

- `environment`
- `catalog`
- `run_date`
- `window_start`
- `window_end`
- `page_size`
- `batch_id`

## Raw Landing Path Pattern

- ADF raw landing file: `abfss://raw@<storage-account>.dfs.core.windows.net/nyc311/service_requests/raw/ingest_date=<run_date>/nyc311_service_requests_<batch_id>.json`
- Databricks handoff `raw_landing_path`: `abfss://raw@<storage-account>.dfs.core.windows.net/nyc311/service_requests/raw/ingest_date=<run_date>/nyc311_service_requests_<batch_id>.json`

## Notebook Parameter Mapping

| ADF pipeline or activity value | Databricks notebook parameter |
| --- | --- |
| `environment` | `environment` |
| `catalog` | `catalog` |
| `run_date` | `run_date` |
| `batch_id` | `batch_id` |
| `window_start` | `window_start` |
| `window_end` | `window_end` |
| constant `adf_landed_raw` | `ingestion_mode` |
| derived ADLS landed file path for the run | `raw_landing_path` |

## Linked Services Used By The Repo-Side Starter Assets

- `ls_nyc311_http`
- `ls_adls_nyc311`
- `ls_databricks_nyc311`

## Notebook Execution Order

- Setup: `01_secrets_and_widgets` -> `00_mounts_and_paths`
- Bronze: `01_ingest_nyc311_raw` -> `02_bronze_dedup_metadata`
- Silver: `01_clean_service_requests` -> `02_standardize_locations` -> `03_standardize_categories` -> `04_apply_quality_rules`
- Gold: dimensions -> `06_build_fact_service_requests` -> marts
- Validation: `01_bronze_validation` -> `02_silver_validation` -> `03_gold_validation`

## Implemented Repo Flow

The current repo implementation combines local Python modules with Databricks notebook exports:

- `src/ingestion/api_extract.py`: paginated API extraction helper
- `src/ingestion/bronze_loader.py`: bronze metadata preparation and lineage path generation
- `src/ingestion/watermark.py`: watermark persistence helpers
- `src/transformation/silver_service_requests.py`: bronze-to-silver parsing and derivations
- `src/quality/`: reusable validation helpers
- `src/transformation/gold_dimensions.py`: gold dimension builders
- `src/transformation/gold_facts.py`: fact-table builder
- `src/transformation/gold_marts.py`: mart aggregations
- `src/common/databricks_runtime.py`: notebook runtime config, ADLS access setup, and table-path resolution

## Milestone 11 Ownership Split

- ADF owns the source extraction window through `window_start` and `window_end`, plus the raw JSON landing.
- Databricks owns the downstream Delta writes and lakehouse-side checkpoint or watermark paths after the handoff.
- `01_ingest_nyc311_raw` intentionally does not advance the source watermark when `ingestion_mode=adf_landed_raw`.

## Repo-Side Orchestration Assets

- `infra/adf/pipeline_nyc311_ingest.json`: repo-side starter definition aligned to the implemented Milestone 11 ADF handoff contract
- `infra/databricks/workflow-job.json`: earlier Databricks task graph and dependency ordering proven in Milestone 10

## Practical Design Intent

- keep the current cloud run honest: ADF now lands the raw extract and hands off to Databricks
- keep bronze metadata-rich and replayable
- isolate cleaning and deduplication in silver
- keep data quality reusable instead of notebook-only
- use dimensions and a fact table to shape the reporting layer
- keep the repo-side ADF and Databricks JSON files lightweight and non-productionized

## Honest Status

- the earlier Milestone 9 and 10 Databricks evidence remains valid
- the Milestone 11 proof added a real ADF raw landing and Databricks handoff, then completed final validation in `dbw-test-centralus-01`
- the final Milestone 11 validation was manual or path-based because the legacy validation notebooks expected registered metastore tables in `spark_catalog`
- Power BI remains a downstream target rather than a completed deliverable
