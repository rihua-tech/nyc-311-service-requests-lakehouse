# NYC 311 Service Requests Lakehouse

> Azure-first medallion lakehouse design for NYC 311 service request analytics.

This repo models and partially implements an end-to-end lakehouse for NYC 311 service request data. The local code covers bronze ingestion preparation, silver cleaning and deduplication, reusable data quality checks, and gold dimensional outputs and reporting marts, while ADF, ADLS Gen2, and Databricks assets document the intended cloud execution path as scaffolded design rather than a finished deployment.

## Project Highlights

- structured NYC 311 service request data into bronze, silver, and gold layers with clear handoffs from raw ingestion to curated analytics outputs
- implemented reusable data quality checks for nulls, duplicates, schema validation, and row-count monitoring
- built gold dimensions, a request-level fact table, and reporting marts for daily volume, service performance, and backlog analysis
- documented an Azure-first execution path using ADF, ADLS Gen2, and Databricks placeholders aligned to the local implementation

## Project Card Copy

- Title: NYC 311 Service Requests Lakehouse
- Subtitle: Azure-first medallion lakehouse design for operational analytics
- Short description: End-to-end NYC 311 lakehouse project with implemented bronze, silver, and gold data processing logic, reusable data quality checks, dimensional modeling, and reporting marts, plus scaffolded Azure and Databricks execution assets.
- Stack tags: Azure Data Factory, Azure Data Lake Storage Gen2, Databricks, PySpark, Python, Delta Lake, SQL, Power BI

## Project Overview

The project models how NYC 311 service request data could be ingested, curated, validated, and shaped for reporting in an Azure lakehouse. The intended target flow is:

```text
NYC 311 API
  -> Azure Data Factory
  -> Azure Data Lake Storage
  -> Databricks / PySpark
  -> Delta Lake bronze / silver / gold
  -> Power BI
```

The goal of the repo is to show how a Data Engineer would structure the problem end to end while staying honest about what is already implemented and what is still scaffolded.

## Architecture Diagram

The saved diagram below gives a quick view of the intended Azure-first lakehouse flow described in this repo.

![NYC 311 lakehouse architecture](docs/architecture/nyc311-lakehouse-architecture.png)

A larger version and supporting notes are available in [docs/architecture/architecture-diagram.md](docs/architecture/architecture-diagram.md).

## Business Problem

NYC 311 data is operationally valuable because it reflects how city services are requested, triaged, and resolved across agencies and neighborhoods. A lakehouse model makes it easier to answer questions such as:

- how many requests are arriving each day
- which agencies and complaint types are driving the most demand
- how long it takes to resolve requests
- where backlog is building up
- which operational metrics are stable enough to publish into a reporting layer

## Current Implementation Status

| Area | Status | Notes |
| --- | --- | --- |
| Local ingestion helpers | Implemented | `src/ingestion/` covers API extraction, watermark scaffolding, and bronze metadata preparation |
| Silver transformations | Implemented | `src/transformation/` cleans requests and builds silver reference outputs |
| Data quality checks | Implemented | reusable null, duplicate, schema, and row-count helpers in `src/quality/` |
| Gold modeling and marts | Implemented | dimensions, fact, marts, and aligned SQL templates are present |
| Local tests | Implemented | unit and integration tests cover the Python helper surface |
| ADF, ADLS, Databricks runtime wiring | Scaffolded | placeholder JSON, notebook order, and runbooks document the intended Azure path |
| Power BI delivery | Scaffolded | represented as the intended downstream consumer, not a finished asset |

This repository is not a completed Azure deployment. Treat the ADF, Databricks, and storage definitions in `infra/` as starter templates rather than live production artifacts.

## What Is Implemented Locally

- [src/ingestion/api_extract.py](src/ingestion/api_extract.py): paginated NYC 311 extraction helper
- [src/ingestion/watermark.py](src/ingestion/watermark.py): local watermark state management
- [src/ingestion/bronze_loader.py](src/ingestion/bronze_loader.py): batch metadata, record hashes, and bronze-style file paths
- [src/transformation/silver_service_requests.py](src/transformation/silver_service_requests.py): request cleaning, timestamp handling, derivations, and deduplication
- [src/transformation/silver_reference_tables.py](src/transformation/silver_reference_tables.py): agency, complaint type, location, and status reference extraction
- [src/quality/](src/quality/): reusable validation helpers for nulls, duplicates, schema checks, and row counts
- [src/transformation/gold_dimensions.py](src/transformation/gold_dimensions.py), [src/transformation/gold_facts.py](src/transformation/gold_facts.py), and [src/transformation/gold_marts.py](src/transformation/gold_marts.py): gold modeling helpers
- [sql/ddl/](sql/ddl/) and [sql/marts/](sql/marts/): SQL mirrors for the modeled tables and marts
- [databricks/notebooks/](databricks/notebooks/): notebook scaffolds aligned to the intended cloud execution order

## What Remains Scaffolded In Azure, Databricks, And Power BI

- ADF deployment, trigger activation, and live secret wiring
- ADLS raw landing and curated Delta writes from a real Azure runtime
- Databricks job execution against cloud storage
- production monitoring, alerting, CI/CD, and infrastructure deployment automation
- Power BI assets beyond documentation placeholders

## Architecture

The repo represents an Azure-first lakehouse architecture with local Python helpers standing in for Databricks runtime logic during the build phase.

Supporting documentation:

- [Architecture Diagram](docs/architecture/architecture-diagram.md)
- [Data Flow](docs/architecture/data-flow.md)
- [Medallion Design](docs/architecture/medallion-design.md)
- [Pipeline Runbook](docs/runbooks/pipeline-runbook.md)

## Medallion Design

- `bronze`: raw API payloads plus ingest metadata such as `ingest_id`, `source_record_id`, `api_pull_timestamp`, `record_hash`, and `file_path`
- `silver`: cleaned request-level records plus reusable reference tables for agencies, complaint types, locations, and statuses
- `gold`: conformed dimensions, a request-level fact table, and business-facing marts for reporting

The local code mirrors the intended layer responsibilities even though the cloud runtime is still scaffolded.

## Gold Outputs And Marts

Gold outputs represented in the repo include:

- dimensions: `gold.dim_date`, `gold.dim_agency`, `gold.dim_complaint_type`, `gold.dim_location`, `gold.dim_status`
- fact table: `gold.fact_service_requests`
- `gold.mart_request_volume_daily`: daily request counts
- `gold.mart_service_performance`: closure counts and average resolution time by agency and complaint type
- `gold.mart_backlog_snapshot`: open backlog counts by snapshot date, status, and agency

The mart definitions are implemented in local helper modules and mirrored in the SQL templates under [sql/marts/](sql/marts/).

## Current Repository Structure

```text
.
|-- config/       # environment placeholders and schema settings
|-- databricks/   # notebook scaffolds and Databricks-side starter assets
|-- docs/         # architecture notes, runbooks, and data dictionaries
|-- infra/        # ADF, Databricks, and Azure placeholder definitions
|-- powerbi/      # documentation placeholders only
|-- sql/          # DDL, marts, and validation SQL templates
|-- src/          # implemented local Python helpers
`-- tests/        # unit and integration tests for the Python modules
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
- [infra/adf/pipeline_nyc311_ingest.json](infra/adf/pipeline_nyc311_ingest.json)
- [infra/databricks/workflow-job.json](infra/databricks/workflow-job.json)

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
- local tests validate the Python helpers, not a running Spark or Azure environment
- the repo currently uses `PyYAML` and `pytest` for local support and tests
- replace placeholder values in [config/dev.yaml](config/dev.yaml) and [config/prod.yaml](config/prod.yaml) before attempting any real cloud wiring
- treat the JSON files in `infra/` as starter templates, not production-ready deployment assets
