# NYC 311 Dashboard Mockup

This file describes the intended Power BI dashboard layout for the starter repo.

## Suggested Pages

### Operations Overview

- daily request volume trend from `gold.mart_request_volume_daily`
- open backlog by status and agency from `gold.mart_backlog_snapshot`
- closed request counts and average resolution time from `gold.mart_service_performance`

### Service Performance

- average resolution time by agency
- closed request counts by agency and complaint type
- top complaint categories by agency

### Backlog Monitoring

- backlog by status
- backlog by agency
- snapshot-date comparison once multiple snapshots exist

## Notes

- no screenshots are included because the dashboard is not implemented yet
- this is a documentation-only planning aid for a future Power BI build
- a geographic page is intentionally deferred because the current marts do not yet publish a location-focused reporting table
