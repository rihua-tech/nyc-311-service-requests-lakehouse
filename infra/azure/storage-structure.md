# Storage Structure

## Suggested Container Layout

```text
raw/
  nyc311/
    service_requests/
      ingest_date=YYYY-MM-DD/

curated/
  bronze/
  silver/
  gold/
```

## Notes

- keep raw landing paths immutable where practical
- use partitioning only where it provides clear operational value
- align ADLS paths with Databricks table ownership conventions

