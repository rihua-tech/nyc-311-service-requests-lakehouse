# Data Flow

## Intended Cloud Path

| Step | Source | Target | Purpose |
| --- | --- | --- | --- |
| 1 | NYC 311 API | ADF HTTP activity | Pull incremental or bounded extracts |
| 2 | ADF | ADLS raw container | Land raw payloads with file-level traceability |
| 3 | ADLS raw | Delta bronze | Preserve raw payloads and ingestion metadata |
| 4 | Bronze | Delta silver | Parse raw JSON, clean fields, and derive operational attributes |
| 5 | Silver | Delta gold | Build dimensions, fact table, and marts |
| 6 | Gold | Power BI | Publish operational reporting outputs |

## Implemented Repo Flow

The repository currently implements the same flow as local Python modules and SQL templates:

- `src/ingestion/api_extract.py`: paginated API extraction helper
- `src/ingestion/bronze_loader.py`: bronze metadata preparation
- `src/transformation/silver_service_requests.py`: bronze-to-silver parsing and derivations
- `src/quality/`: reusable validation helpers
- `src/transformation/gold_dimensions.py`: gold dimension builders
- `src/transformation/gold_facts.py`: fact-table builder
- `src/transformation/gold_marts.py`: mart aggregations

## Practical Design Intent

- keep bronze replayable and metadata-rich
- isolate cleaning and deduplication in silver
- keep data quality reusable instead of notebook-only
- use dimensions and a fact table to shape the reporting layer
- expose marts that are understandable by non-engineering stakeholders

## Honest Status

- the Azure and Databricks runtime path is still scaffolded
- the local Python modules are the most concrete implementation in the repo today
- Power BI is documented as a next-stage consumer, not an implemented asset
