# Metrics Definition

## Starter Metrics

### Request Volume

- definition: count of service requests created during the reporting period
- candidate source: `gold.mart_request_volume_daily`

### Average Resolution Time

- definition: average hours between request creation and closure for closed requests
- candidate source: `gold.mart_service_performance`

### Open Backlog

- definition: count of requests not yet closed at snapshot time
- candidate source: `gold.mart_backlog_snapshot`

## TODO

- confirm whether overdue backlog should be tracked separately
- define complaint-level SLAs if that becomes part of the reporting scope
