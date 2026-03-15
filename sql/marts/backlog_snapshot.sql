-- Starter mart template for backlog snapshot reporting.

SELECT
    CURRENT_DATE AS snapshot_date,
    status_name,
    agency_code,
    COUNT(*) AS open_request_count
FROM gold.fact_service_requests
WHERE is_closed = FALSE
GROUP BY status_name, agency_code;

