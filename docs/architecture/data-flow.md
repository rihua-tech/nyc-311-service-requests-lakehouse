# Data Flow

## Planned Movement

| Step | Source | Target | Purpose |
| --- | --- | --- | --- |
| 1 | NYC 311 API | ADF HTTP activity | Pull incremental or bounded extracts |
| 2 | ADF | ADLS raw container | Land raw payloads with file-level traceability |
| 3 | ADLS raw | Delta bronze | Preserve source payload and ingest metadata |
| 4 | Bronze | Delta silver | Standardize request fields and derive operational flags |
| 5 | Silver | Delta gold | Build dimensions, facts, and reporting marts |
| 6 | Gold | Power BI | Support operational analytics and KPI reporting |

## Design Intent

- keep the bronze layer replayable
- isolate cleaning logic in silver
- publish stable business-facing models in gold
- retain enough metadata for reconciliation and debugging

## TODO

- confirm the API endpoint contract and pagination pattern
- choose the incremental watermark column
- define the final storage account and container naming standard

