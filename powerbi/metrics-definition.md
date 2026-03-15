# Metrics Definition

These definitions describe the marts currently implemented in code and SQL. They are suitable for a starter dashboard, but they are not yet backed by a deployed Power BI model.

## `gold.mart_request_volume_daily`

### Request Volume

- definition: count of fact rows grouped by `created_date`
- grain: one row per request creation date
- source logic: `gold.fact_service_requests` grouped by `created_date`
- published field: `request_count`

## `gold.mart_service_performance`

### Closed Requests

- definition: count of closed fact rows grouped by `agency_code` and `complaint_type`
- grain: one row per agency and complaint type
- source logic: `gold.fact_service_requests` filtered to `is_closed = true`
- published field: `closed_requests`

### Average Resolution Time

- definition: average non-negative `resolution_time_hours` for closed requests in each agency and complaint-type group
- grain: one row per agency and complaint type
- source logic: `gold.fact_service_requests` filtered to `is_closed = true`
- published field: `avg_resolution_time_hours`

## `gold.mart_backlog_snapshot`

### Open Backlog

- definition: count of requests considered open for the chosen snapshot date, grouped by `status_name` and `agency_code`
- grain: one row per snapshot date, status, and agency
- source logic: `gold.fact_service_requests` filtered to open requests and grouped by `status_name` and `agency_code`
- published field: `open_request_count`

## Notes

- backlog snapshot logic is currently based on the starter fact-table model, not a full historical status event model
- overdue backlog is not exposed as a separate mart yet
- SLA metrics and percentile-based service metrics are not implemented yet
