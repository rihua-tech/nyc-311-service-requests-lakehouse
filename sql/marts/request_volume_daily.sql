-- Starter mart template for daily request volume.

SELECT
    COALESCE(d.calendar_date, f.created_date) AS created_date,
    COUNT(*) AS request_count
FROM gold.fact_service_requests AS f
LEFT JOIN gold.dim_date AS d
    ON f.created_date_key = d.date_key
WHERE f.created_date IS NOT NULL
GROUP BY COALESCE(d.calendar_date, f.created_date)
ORDER BY created_date;
