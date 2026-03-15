# Silver Data Dictionary

## `silver.service_requests_clean`

Grain: one cleaned service request record after bronze parsing and duplicate handling.

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

## `silver.agency_reference`

Grain: one unique agency code from the processed silver batch.

| Column | Description |
| --- | --- |
| `agency_code` | Agency code from the silver request table |
| `agency_name` | Agency display name |
| `first_seen_created_date` | Earliest created date observed for the agency in the processed batch |
| `load_timestamp` | Timestamp when the reference row was produced |

## `silver.complaint_type_reference`

Grain: one unique complaint type and descriptor pair from the processed silver batch.

| Column | Description |
| --- | --- |
| `complaint_type` | Complaint category |
| `descriptor` | Complaint descriptor or subtype |
| `first_seen_created_date` | Earliest created date observed for the complaint type in the processed batch |
| `load_timestamp` | Timestamp when the reference row was produced |

## `silver.location_reference`

Grain: one unique location key based on ZIP, borough, city, and location type.

| Column | Description |
| --- | --- |
| `incident_zip` | Incident ZIP code |
| `borough` | Canonical borough value |
| `city` | City or locality value |
| `location_type` | Location type such as street or residential building |
| `latitude` | Representative latitude from the processed batch |
| `longitude` | Representative longitude from the processed batch |
| `load_timestamp` | Timestamp when the reference row was produced |

## `silver.status_reference`

Grain: one unique request status from the processed silver batch.

| Column | Description |
| --- | --- |
| `status_name` | Normalized request status |
| `status_group` | Starter grouping such as `Open` or `Closed` |
| `is_closed_status` | Boolean helper indicating whether the status represents closure |
| `load_timestamp` | Timestamp when the reference row was produced |
