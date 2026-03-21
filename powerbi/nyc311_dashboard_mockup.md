# NYC 311 Dashboard Mockup

This file describes the intended Power BI dashboard layout for the current repo state. It is a documentation-only mockup aligned to the implemented gold marts, not a claim that a finished Power BI asset already exists.

## Recommended Page Set

### 1. Request Volume Trends

- primary dataset: `gold.mart_request_volume_daily`
- business question: how many requests are arriving over time and whether demand is increasing or stabilizing

Recommended visuals:

- line chart of `request_count` by `created_date`
- KPI card for total requests in the selected date range
- simple date slicer for daily, weekly, or monthly review windows

Recommended metric labels:

- `Request Volume`
- `Selected Period Requests`

### 2. Service Performance

- primary dataset: `gold.mart_service_performance`
- business question: which agencies and complaint categories are closing the most work and how long resolution is taking

Recommended visuals:

- clustered bar chart of `closed_requests` by `agency_code`
- matrix with `agency_code`, `complaint_type`, `closed_requests`, and `avg_resolution_time_hours`
- ranked bar chart for the highest average resolution time groups

Recommended metric labels:

- `Closed Requests`
- `Average Resolution Time (Hours)`

Reporting note:

- the current mart is best used at `agency_code` plus `complaint_type` grain; report-level rollups of average resolution time should be treated carefully

### 3. Backlog Snapshot

- primary dataset: `gold.mart_backlog_snapshot`
- business question: what open workload exists at the selected snapshot date and where it is concentrated

Recommended visuals:

- KPI card for total `open_request_count` at the selected `snapshot_date`
- stacked bar chart of `open_request_count` by `status_name`
- bar chart of `open_request_count` by `agency_code`
- snapshot date slicer for comparing retained backlog snapshots once multiple dates exist

Recommended metric label:

- `Open Request Count`

### 4. Borough / Complaint Type Breakdown

- current status: recommended follow-on page, not fully supported by the current mart layer
- reason: the existing marts do not yet publish borough-level aggregates
- practical guidance: keep this page in the mockup as a next reporting extension rather than presenting it as a finished, mart-backed page
- clean implementation path: add a borough-oriented gold mart or expose a reviewed semantic-model table using `gold.fact_service_requests` with `gold.dim_location`
- recruiter-friendly takeaway: the reporting roadmap is visible, but the repo does not overclaim a borough dashboard that the current marts cannot fully support

## Suggested Reviewer Narrative

- start on `Request Volume Trends` to establish incoming demand
- move to `Service Performance` to show agency and complaint-type resolution patterns
- use `Backlog Snapshot` to show current operational pressure
- treat `Borough / Complaint Type Breakdown` as the next logical reporting enhancement once the gold layer adds borough-grain reporting support

## Notes

- no screenshots are included because the dashboard is not implemented yet
- this is a documentation-only planning aid for a future Power BI build
- the current dashboard mockup is intentionally aligned to `gold.mart_request_volume_daily`, `gold.mart_service_performance`, and `gold.mart_backlog_snapshot`
