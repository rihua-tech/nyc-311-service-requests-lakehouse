# Gold Data Dictionary

## Dimensions

- `gold.dim_date`
- `gold.dim_agency`
- `gold.dim_complaint_type`
- `gold.dim_location`
- `gold.dim_status`

## Fact

- `gold.fact_service_requests`

## Marts

- `gold.mart_request_volume_daily`
- `gold.mart_service_performance`
- `gold.mart_backlog_snapshot`

## Modeling Notes

- Dimensions are intended to support common Power BI slicers.
- The fact table should remain close to request-level analytical grain.
- Gold marts should stay purpose-built for reporting rather than duplicate all silver columns.

## TODO

- finalize metric definitions with reporting requirements
- document surrogate keys and foreign key relationships
- decide which marts should be tables versus SQL views

