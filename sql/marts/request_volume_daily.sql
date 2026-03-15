-- Starter mart template for daily request volume.

SELECT
    created_date,
    COUNT(*) AS request_count
FROM gold.fact_service_requests
GROUP BY created_date
ORDER BY created_date;

