# Data Flow

## Intended Azure Execution Path

| Step | Source | Target | Purpose |
| --- | --- | --- | --- |
| 1 | NYC 311 API | ADF trigger or manual pipeline run | Define a bounded source extract for the requested run window |
| 2 | `pl_nyc311_ingest` | `CopyNYC311ToBronzeRaw` | Pull `/resource/erm2-nwe9.json` for the selected `run_date`, `window_start`, and `window_end` |
| 3 | ADF copy activity | ADLS `raw` bronze landing path | Write raw JSON batches under `nyc311/service_requests/raw/ingest_date=YYYY-MM-DD/` |
| 4 | ADF handoff activity | Databricks workflow `wf_nyc311_lakehouse` | Pass control, `run_date`, raw landing path, and checkpoint path into Databricks |
| 5 | Databricks bronze notebooks | `bronze.nyc311_service_requests_raw` | Ingest raw files and confirm bronze metadata or dedup handling |
| 6 | Databricks silver notebooks | `silver.service_requests_clean` and silver reference tables | Clean service requests, standardize location and category fields, and apply silver quality rules |
| 7 | Databricks gold notebooks | Gold dimensions, fact table, and marts | Build curated reporting-ready outputs |
| 8 | Databricks validation notebooks | Bronze, silver, and gold validation summaries | Check layer-level completeness and reconciliation after the processing chain |
| 9 | Gold outputs | Power BI or downstream SQL consumers | Publish operational reporting outputs |

## Databricks Execution Order

- Bronze: `01_ingest_nyc311_raw` -> `02_bronze_dedup_metadata`
- Silver: `01_clean_service_requests` -> `02_standardize_locations` and `03_standardize_categories` -> `04_apply_quality_rules`
- Gold: dimensions -> `06_build_fact_service_requests` -> marts
- Validation: `01_bronze_validation` -> `02_silver_validation` -> `03_gold_validation`

## Implemented Repo Flow

The repository currently mirrors the same path through local Python modules, notebook exports, and placeholder infrastructure templates:

- `src/ingestion/api_extract.py`: paginated API extraction helper
- `src/ingestion/bronze_loader.py`: bronze metadata preparation
- `src/transformation/silver_service_requests.py`: bronze-to-silver parsing and derivations
- `src/quality/`: reusable validation helpers
- `src/transformation/gold_dimensions.py`: gold dimension builders
- `src/transformation/gold_facts.py`: fact-table builder
- `src/transformation/gold_marts.py`: mart aggregations
- `infra/adf/pipeline_nyc311_ingest.json`: intended ADF ingestion and Databricks handoff shape
- `infra/databricks/workflow-job.json`: intended Databricks execution order across bronze, silver, gold, and validation

## Practical Design Intent

- keep ADF responsible for source extraction and raw landing
- keep Databricks responsible for medallion processing and validation
- keep bronze replayable and metadata-rich
- isolate cleaning and deduplication in silver
- keep data quality reusable instead of notebook-only
- use dimensions and a fact table to shape the reporting layer
- expose marts that are understandable by non-engineering stakeholders
- keep checkpoints separate from curated silver and gold outputs

## Honest Status

- the ADF and Databricks runtime path is still scaffolded and documented rather than deployed
- the local Python modules are the most concrete implementation in the repo today
- Power BI is documented as a next-stage consumer, not an implemented asset
