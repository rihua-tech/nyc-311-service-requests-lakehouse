# Silver Data Dictionary

## `silver.service_requests_clean`

| Column | Description |
| --- | --- |
| `request_id` | Standardized service request identifier |
| `created_at` | Request creation timestamp |
| `closed_at` | Request closure timestamp |
| `updated_at` | Last known update timestamp |
| `due_date` | Due date from source when present |
| `agency_code` | Agency code from source |
| `agency_name` | Agency name from source |
| `complaint_type` | High-level complaint category |
| `descriptor` | Source descriptor or subtype |
| `status_name` | Current request status |
| `borough` | Borough name |
| `incident_zip` | ZIP code associated with the incident |
| `city` | City or locality field |
| `latitude` | Parsed latitude |
| `longitude` | Parsed longitude |
| `location_type` | Normalized location type |
| `channel_type` | Submission channel when available |
| `resolution_description` | Closure or action narrative |
| `resolution_time_hours` | Derived duration from creation to closure |
| `is_closed` | Boolean flag for closed requests |
| `is_overdue` | Boolean flag for overdue requests |
| `created_year` | Derived year partition field |
| `created_month` | Derived month partition field |
| `created_date` | Date portion of creation timestamp |
| `closed_date` | Date portion of closure timestamp |
| `record_hash` | Hash used for change detection or deduplication |
| `load_timestamp` | Timestamp when the silver row was created |

## Reference Tables

- `silver.agency_reference`
- `silver.complaint_type_reference`
- `silver.location_reference`
- `silver.status_reference`

