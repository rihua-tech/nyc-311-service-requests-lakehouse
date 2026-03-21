# Metrics Definition

These definitions describe the gold marts currently implemented in code and SQL. They are suitable for a starter dashboard mockup, but they are not yet backed by a deployed Power BI semantic model.

## Current Reporting Datasets

| Dataset | Grain | Best Business Use |
| --- | --- | --- |
| `gold.mart_request_volume_daily` | one row per `created_date` | daily demand trend and selected-period request volume |
| `gold.mart_service_performance` | one row per `agency_code` and `complaint_type` | closure volume and resolution-time comparison across agencies and complaint categories |
| `gold.mart_backlog_snapshot` | one row per `snapshot_date`, `status_name`, and `agency_code` | open backlog monitoring for a selected snapshot date |

## `gold.mart_request_volume_daily`

### Request Volume

- business meaning: how many NYC 311 requests were created in the selected reporting period
- definition: sum of `request_count` over the selected date filter context
- mart grain: one row per request creation date
- source logic: `gold.fact_service_requests` grouped by `created_date`
- published field: `request_count`
- best report use: line trends, date-sliced KPI cards, and simple period-over-period volume comparisons
- note: this mart does not currently expose agency, complaint type, or borough breakdowns

## `gold.mart_service_performance`

### Closed Requests

- business meaning: how many requests were closed for a given agency and complaint type
- definition: sum of `closed_requests` over the selected agency and complaint-type filter context
- mart grain: one row per agency and complaint type
- source logic: `gold.fact_service_requests` filtered to `is_closed = true`
- published field: `closed_requests`
- best report use: ranked agency tables, complaint-type matrices, and top-N closure volume views

### Average Resolution Time

- business meaning: how long closed requests took to resolve for a given agency and complaint type
- definition: average non-negative `resolution_time_hours` for closed requests in each published group
- mart grain: one row per agency and complaint type
- source logic: `gold.fact_service_requests` filtered to `is_closed = true`
- published field: `avg_resolution_time_hours`
- best report use: side-by-side comparison of agency and complaint-type groups at the mart grain
- note: averaging `avg_resolution_time_hours` across multiple mart rows can be misleading because the current mart does not publish a weighted numerator and denominator pair for rollups

## `gold.mart_backlog_snapshot`

### Open Backlog

- business meaning: how many requests were still open at the selected snapshot date
- definition: sum of `open_request_count` for the selected `snapshot_date`
- mart grain: one row per snapshot date, status, and agency
- source logic: `gold.fact_service_requests` filtered to open requests and grouped by `status_name` and `agency_code`
- published field: `open_request_count`
- best report use: backlog cards, status mix visuals, and agency backlog comparisons
- note: snapshot comparisons become more informative only after multiple retained snapshot dates exist in the reporting layer

## Not Currently Modeled In The Mart Layer

- a borough-level reporting mart is not implemented yet, so a clean borough dashboard page would require a follow-on mart or a reviewed semantic-model table based on gold facts and dimensions
- overdue backlog is not exposed as a separate mart yet
- SLA attainment, percentile-based service metrics, and weighted rolled-up resolution-time measures are not implemented yet
- backlog snapshot logic is currently based on the starter fact-table model, not a full historical status event model
