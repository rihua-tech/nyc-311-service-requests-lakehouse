# Bronze Data Dictionary

## `bronze.nyc311_service_requests_raw`

Grain: one landed source record per ingest event.

| Column | Description |
| --- | --- |
| `ingest_id` | Unique identifier for a load event row |
| `source_system` | Source label such as `nyc_311_api` |
| `source_record_id` | Upstream request identifier when available |
| `ingest_timestamp` | UTC timestamp when the row entered bronze |
| `ingest_date` | Date portion of the ingest timestamp |
| `api_pull_timestamp` | Timestamp associated with the extraction request |
| `raw_payload` | Raw JSON payload as a string |
| `record_hash` | Hash of the landed payload for deduplication support |
| `file_path` | ADLS landing path for the raw file or batch |

## Notes

- Bronze should remain as close to source as practical.
- The current local ingestion helpers also maintain a watermark state file, but that watermark is not stored in the bronze table itself.
- Any parsing beyond metadata capture belongs in silver.
