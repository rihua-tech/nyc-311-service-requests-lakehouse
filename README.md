# NYC 311 Service Requests Lakehouse

End-to-end Azure + Databricks lakehouse pipeline for operational analytics on NYC 311 service request data.

## Project Status

This repository is an in-progress portfolio project with implemented local foundations for ingestion, transformation, data quality, gold modeling, and mart logic. It is not a completed Azure deployment. The Azure Data Factory, Databricks job, and Power BI delivery assets are still scaffolded and documented rather than fully operational.

## Project Overview

The project models a practical operational analytics pipeline for NYC 311 service requests. The intended production shape is:

```text
NYC 311 API
  -> Azure Data Factory
  -> Azure Data Lake Storage
  -> Databricks / PySpark
  -> Delta Lake bronze / silver / gold
  -> Power BI
```

The repository currently focuses on making that design believable and extendable:

- configurable API extraction helpers and local watermark handling
- bronze record preparation with ingest metadata and lineage fields
- silver-layer parsing, normalization, and deduplication logic
- reusable data quality helpers for nulls, duplicates, schema checks, and row counts
- gold dimensions, fact-table helpers, and reporting mart aggregations
- notebook scaffolds, SQL templates, and documentation aligned to the implemented Python modules

## Business Problem

NYC 311 data is operationally valuable because it reflects how city services are requested, triaged, and resolved across agencies and neighborhoods. A lakehouse model makes it easier to answer questions such as:

- how many requests are arriving each day
- which agencies and complaint types are driving the most demand
- how long it takes to resolve requests
- where backlog is building up
- which operational metrics are stable enough to publish into a reporting layer

This repo is designed to showcase how a Data Engineer would structure that problem end to end, while staying honest about what is and is not fully implemented yet.

## Architecture

The repo represents an Azure-first lakehouse architecture with local Python helpers standing in for Databricks job logic during the build phase.

Supporting documentation:

- [Architecture Diagram](docs/architecture/architecture-diagram.md)
- [Data Flow](docs/architecture/data-flow.md)
- [Medallion Design](docs/architecture/medallion-design.md)

## Medallion Design

- `bronze`: raw API payloads plus ingest metadata such as `ingest_id`, `source_record_id`, `api_pull_timestamp`, `record_hash`, and `file_path`
- `silver`: cleaned request-level records plus reusable reference tables for agencies, complaint types, locations, and statuses
- `gold`: conformed dimensions, a request-level fact table, and business-facing marts for reporting

The local code mirrors the intended layer responsibilities even though the Databricks runtime implementation is still scaffolded.

## Ingestion Flow

The ingestion layer is implemented as lightweight local helpers today.

- [src/ingestion/api_extract.py](src/ingestion/api_extract.py) builds Socrata-style pagination parameters and fetches pages from the NYC 311 endpoint
- [src/ingestion/watermark.py](src/ingestion/watermark.py) reads and writes a local watermark state file for incremental-load scaffolding
- [src/ingestion/bronze_loader.py](src/ingestion/bronze_loader.py) attaches ingest metadata, record hashes, and partition-like bronze file paths
- [config/dev.yaml](config/dev.yaml) contains placeholder dev settings for endpoint, pagination, watermark field, and bronze landing paths

What is not done yet:

- no live ADF orchestration
- no ADLS write implementation
- no Databricks job execution against cloud storage

## Silver Transformation Layer

The silver layer has implemented local transformation logic in [src/transformation/silver_service_requests.py](src/transformation/silver_service_requests.py) and [src/transformation/silver_reference_tables.py](src/transformation/silver_reference_tables.py).

Current behavior includes:

- parsing bronze `raw_payload`
- selecting core service request fields
- timestamp casting and date derivation
- borough normalization
- `is_closed`, `is_overdue`, and `resolution_time_hours` derivation
- duplicate `request_id` handling
- reference extraction for agency, complaint type, location, and status dimensions

## Data Quality Layer

The data quality layer is implemented as reusable helper modules under `src/quality/`.

- null checks for required fields and null-count summaries
- duplicate checks for keys such as `request_id`
- schema checks for expected columns and simple runtime types
- row-count reconciliation summaries
- validation notebook scaffolds and SQL validation queries aligned to bronze, silver, and gold

## Gold Dimensions And Fact Table

The gold modeling layer is implemented in local helper modules and DDL/templates.

- [src/transformation/gold_dimensions.py](src/transformation/gold_dimensions.py) builds `dim_date`, `dim_agency`, `dim_complaint_type`, `dim_location`, and `dim_status`
- [src/transformation/gold_facts.py](src/transformation/gold_facts.py) builds `gold.fact_service_requests`
- the current surrogate-key strategy is a starter pattern based on stable sorting of business keys, with `0` used as an unknown-key fallback in the fact builder

The fact table retains a few business columns such as `agency_code`, `complaint_type`, and `status_name` alongside surrogate keys so the repo stays readable and easy to extend.

## Gold Marts

The mart layer is implemented in [src/transformation/gold_marts.py](src/transformation/gold_marts.py).

Current marts:

- `gold.mart_request_volume_daily`: request counts by `created_date`
- `gold.mart_service_performance`: closed request counts and average resolution time by `agency_code` and `complaint_type`
- `gold.mart_backlog_snapshot`: open request counts by `snapshot_date`, `status_name`, and `agency_code`

The SQL templates under `sql/marts/` mirror the same mart definitions using the current fact-and-dimension model.

## Current Repository Structure

```text
.
|-- config/
|-- databricks/
|-- docs/
|-- infra/
|-- powerbi/
|-- sql/
|-- src/
`-- tests/
```

Useful entry points:

- [src/common/config_loader.py](src/common/config_loader.py)
- [src/ingestion/api_extract.py](src/ingestion/api_extract.py)
- [src/transformation/silver_service_requests.py](src/transformation/silver_service_requests.py)
- [src/quality/null_checks.py](src/quality/null_checks.py)
- [src/transformation/gold_dimensions.py](src/transformation/gold_dimensions.py)
- [src/transformation/gold_facts.py](src/transformation/gold_facts.py)
- [src/transformation/gold_marts.py](src/transformation/gold_marts.py)
- [sql/ddl/gold_tables.sql](sql/ddl/gold_tables.sql)

## Setup Notes

This repo targets Python 3.11 and keeps local dependencies intentionally small.

```bash
python -m venv .venv
.venv\Scripts\activate
python -m pip install -r requirements.txt
python -m pytest
```

Optional shortcuts:

```bash
make install
make test
```

Notes:

- notebook files are `.py` exports used as scaffolds, not finished production notebooks
- local tests validate the helper modules, not a running Spark or Azure environment
- the repo currently uses `PyYAML` and `pytest` only for local support and tests

## Azure And Databricks Notes

- replace placeholder values in [config/dev.yaml](config/dev.yaml) and [config/prod.yaml](config/prod.yaml)
- map ADLS containers and storage paths before wiring cloud execution
- wire secrets through Databricks secret scopes or Azure Key Vault
- update ADF linked services, datasets, and triggers with real resource names
- treat the JSON files in `infra/` as starter templates, not production-ready deployment assets

## Honest Status And Next Steps

What is implemented now:

- local ingestion, transformation, quality, gold-model, and mart helper logic
- aligned SQL templates, data dictionaries, and notebook scaffolds
- starter tests covering the local Python surface

What is not implemented yet:

- live Azure Data Factory orchestration
- actual ADLS landing and Delta writes
- Databricks DataFrame jobs that replace the local list/dict helper logic
- deployed Power BI assets
- CI/CD and infrastructure deployment automation

Pragmatic next steps:

1. Port the local bronze, silver, gold, and mart helpers into Spark DataFrame logic inside Databricks notebooks or jobs.
2. Wire the ingestion flow to ADLS and persist Delta tables in the intended schemas.
3. Add environment-aware orchestration and operational monitoring.
4. Finalize mart definitions against real dashboard requirements before building Power BI assets.
