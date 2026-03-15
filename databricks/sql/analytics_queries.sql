-- Starter Databricks SQL queries for exploratory analysis.

SELECT
    created_date,
    COUNT(*) AS request_count
FROM gold.fact_service_requests
GROUP BY created_date
ORDER BY created_date DESC;

-- TODO: Add agency and complaint-type trend queries after gold marts are stable.

