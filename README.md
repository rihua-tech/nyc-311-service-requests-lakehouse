# NYC 311 Service Requests Lakehouse

End-to-end Azure + Databricks lakehouse pipeline for operational analytics on NYC 311 service request data.

## Project Status

This repository is a scaffold-first, in-progress portfolio project. It is intentionally structured like a serious Data Engineering codebase, but the ingestion, transformation, infrastructure, and reporting layers are only lightly stubbed so they can be extended incrementally.

## Project Overview

The goal of this project is to ingest NYC 311 service request data from an external API, land raw payloads in Azure Data Lake Storage, process the data in Databricks with PySpark, model the data using a medallion architecture, and prepare curated gold tables for Power BI reporting.

## Why This Project Matters

NYC 311 data is useful for operational analytics because it captures service demand, resolution patterns, backlog pressure, and geographic trends across city agencies. A lakehouse implementation provides a practical way to demonstrate:

- incremental ingestion patterns
- cloud storage design
- medallion-layer transformation thinking
- data quality controls
- dimensional modeling for reporting

## Architecture

```text
External API
  -> Azure Data Factory
  -> Azure Data Lake Storage
  -> Databricks / PySpark
  -> Delta Lake bronze / silver / gold
  -> Power BI
```

Supporting documentation:

- [Architecture Diagram](docs/architecture/architecture-diagram.md)
- [Data Flow](docs/architecture/data-flow.md)
- [Medallion Design](docs/architecture/medallion-design.md)

## Medallion Design

- `bronze`: raw API payloads plus ingestion metadata for replay and traceability
- `silver`: standardized request records and lightweight reference tables
- `gold`: dimensions, fact tables, and reporting marts for Power BI

The current scaffold documents the table targets, naming conventions, notebook flow, and DDL templates without claiming that the pipeline is fully implemented.

## Repository Structure

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

Key entry points:

- [src/common/config_loader.py](src/common/config_loader.py)
- [src/ingestion/api_extract.py](src/ingestion/api_extract.py)
- [src/transformation/silver_service_requests.py](src/transformation/silver_service_requests.py)
- [sql/ddl/gold_tables.sql](sql/ddl/gold_tables.sql)

## Planned Gold Tables

- `gold.dim_date`
- `gold.dim_agency`
- `gold.dim_complaint_type`
- `gold.dim_location`
- `gold.dim_status`
- `gold.fact_service_requests`
- `gold.mart_request_volume_daily`
- `gold.mart_service_performance`
- `gold.mart_backlog_snapshot`

## Planned Data Quality Checks

- required-field null checks on business-critical columns
- duplicate detection on request identifiers
- schema conformance checks between raw and curated layers
- row-count reconciliation across pipeline stages
- validation notebooks and SQL query templates for manual review

## Local Development Notes

This repo targets Python 3.11 for local utilities and starter tests.

```bash
python -m venv .venv
.venv\Scripts\activate
python -m pip install -r requirements.txt
python -m pytest
```

The Python code is intentionally lightweight. It is meant to support local scaffolding, unit tests, and notebook structure rather than replace Databricks runtime behavior.

## Azure And Databricks Setup Notes

- replace placeholder values in [config/dev.yaml](config/dev.yaml) and [config/prod.yaml](config/prod.yaml)
- map ADLS containers and storage paths before writing ingestion logic
- wire secrets through Databricks secret scopes or Azure Key Vault
- update ADF linked services, datasets, and triggers with real resource names
- treat the JSON files in `infra/` as starter templates, not deployable assets

## Next Steps

1. Finalize the API extraction contract and incremental watermark strategy.
2. Replace placeholder Databricks notebook steps with PySpark transformations.
3. Expand SQL DDL and marts based on agreed reporting grain and KPIs.
4. Add deployment automation only after the data model and pipeline flow stabilize.
