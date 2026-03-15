-- Starter mart template for service performance.

SELECT
    agency_code,
    complaint_type,
    COUNT(*) AS closed_requests,
    AVG(resolution_time_hours) AS avg_resolution_time_hours
FROM gold.fact_service_requests
WHERE is_closed = TRUE
GROUP BY agency_code, complaint_type;

