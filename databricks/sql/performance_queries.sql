-- Starter Databricks SQL queries for performance-oriented reporting.

SELECT
    agency_code,
    AVG(resolution_time_hours) AS avg_resolution_time_hours
FROM gold.fact_service_requests
WHERE is_closed = TRUE
GROUP BY agency_code
ORDER BY avg_resolution_time_hours ASC;

-- TODO: Add percentile-based service-time analysis if required.

