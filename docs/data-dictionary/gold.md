# Gold Data Dictionary

## Dimensions

### `gold.dim_date`

Grain: one calendar date.

Key columns:

- `date_key`
- `calendar_date`
- `calendar_year`
- `calendar_quarter`
- `calendar_month`
- `month_name`
- `calendar_day`
- `day_of_week`
- `day_name`
- `is_weekend`

### `gold.dim_agency`

Grain: one agency code.

Key columns:

- `agency_sk`
- `agency_code`
- `agency_name`
- `first_seen_created_date`
- `effective_timestamp`

### `gold.dim_complaint_type`

Grain: one complaint type and descriptor pair.

Key columns:

- `complaint_type_sk`
- `complaint_type`
- `descriptor`
- `first_seen_created_date`
- `effective_timestamp`

### `gold.dim_location`

Grain: one location key based on ZIP, borough, city, and location type.

Key columns:

- `location_sk`
- `incident_zip`
- `borough`
- `city`
- `location_type`
- `latitude`
- `longitude`
- `effective_timestamp`

### `gold.dim_status`

Grain: one normalized request status.

Key columns:

- `status_sk`
- `status_name`
- `status_group`
- `is_closed_status`
- `effective_timestamp`

## Fact

### `gold.fact_service_requests`

Grain: one service request record.

Key columns:

- surrogate keys: `created_date_key`, `closed_date_key`, `agency_sk`, `complaint_type_sk`, `location_sk`, `status_sk`
- degenerate/business fields retained for traceability: `request_id`, `agency_code`, `complaint_type`, `descriptor`, `status_name`, `incident_zip`, `borough`, `city`, `location_type`
- measures and flags: `resolution_time_hours`, `is_closed`, `is_overdue`
- lineage and audit fields: `record_hash`, `load_timestamp`

## Marts

### `gold.mart_request_volume_daily`

Grain: one row per `created_date`.

| Column | Description |
| --- | --- |
| `created_date` | Request creation date |
| `request_count` | Count of request facts created on that date |

### `gold.mart_service_performance`

Grain: one row per `agency_code` and `complaint_type`.

| Column | Description |
| --- | --- |
| `agency_code` | Agency code used for grouping |
| `complaint_type` | Complaint type used for grouping |
| `closed_requests` | Count of closed request facts in the group |
| `avg_resolution_time_hours` | Average of non-negative `resolution_time_hours` for closed requests in the group |

### `gold.mart_backlog_snapshot`

Grain: one row per `snapshot_date`, `status_name`, and `agency_code`.

| Column | Description |
| --- | --- |
| `snapshot_date` | Date the backlog snapshot represents |
| `status_name` | Status label used in the open-request grouping |
| `agency_code` | Agency code used in the open-request grouping |
| `open_request_count` | Count of open request facts included in the snapshot |

## Modeling Notes

- Dimensions are intended to support common Power BI slicers.
- The fact table should remain close to request-level analytical grain.
- Gold marts intentionally stay narrow and metric-focused.
- The current surrogate-key strategy is a starter implementation, not a final warehouse standard.
- Historical status tracking is not implemented, so backlog snapshot logic should be treated as a pragmatic reporting approximation rather than a full event-sourced backlog model.
