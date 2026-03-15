# Troubleshooting

## Common Areas To Check

### API Ingestion

- invalid endpoint or missing query parameters
- pagination logic not aligned with source behavior
- upstream throttling or transient API failures

### Storage Landing

- incorrect ADLS container path
- missing write permissions
- unexpected file naming or date partition layout

### Databricks Processing

- schema drift between raw payloads and silver parsing logic
- missing secrets or cluster configuration issues
- duplicate records caused by watermark gaps

### Reporting Layer

- mart grain not aligned with Power BI expectations
- validation queries showing row-count mismatches
- stale dimension mappings after reference updates

## TODO

- attach exact commands and dashboards once observability exists
- capture common failure signatures from real test runs

