# Data Flow

## Current Milestone 9 Cloud Execution Path

| Step | Source | Target | Purpose |
| --- | --- | --- | --- |
| 1 | Databricks setup notebooks | runtime widgets, secret scope, catalog session | validate environment inputs, secret lookups, and catalog access |
| 2 | `01_ingest_nyc311_raw` | `bronze.nyc311_service_requests_raw` | call the NYC 311 API directly from Databricks, attach ingest metadata, and write the bronze Delta table |
| 3 | `01_ingest_nyc311_raw` | ADLS watermark path under `bronze/checkpoints/` | persist the latest source watermark for the next incremental pull |
| 4 | `02_bronze_dedup_metadata` | `bronze.nyc311_service_requests_raw` | remove duplicate bronze records and republish the bronze table |
| 5 | silver notebooks | `silver.service_requests_clean` and silver reference tables | clean request fields, standardize locations and categories, and run silver quality rules |
| 6 | gold notebooks | gold dimensions, fact table, and marts | build reporting-ready gold outputs |
| 7 | validation notebooks | printed bronze, silver, and gold summaries | confirm completeness, quality, and reconciliation after processing |
| 8 | gold outputs | future Power BI or SQL consumers | expose reporting-friendly datasets for downstream analysis |

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

## Future Orchestration Path Still In Repo

- `infra/adf/pipeline_nyc311_ingest.json`: planned ADF ingestion and orchestration shape
- `infra/databricks/workflow-job.json`: planned Databricks task graph and dependency ordering

## Practical Design Intent

- keep the current cloud run honest: Databricks executes the pipeline directly today
- keep bronze metadata-rich and replayable
- isolate cleaning and deduplication in silver
- keep data quality reusable instead of notebook-only
- use dimensions and a fact table to shape the reporting layer
- leave ADF and workflow deployment notes in the repo for the next phase without pretending they are already deployed

## Honest Status

- the manual Databricks-to-ADLS path is real and evidenced in `docs/screenshots/milestone-9/`
- ADF orchestration and deployed Databricks jobs are still scaffolded
- Power BI remains a downstream target rather than a completed deliverable
