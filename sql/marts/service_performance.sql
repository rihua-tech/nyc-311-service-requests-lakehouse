-- Starter mart template for service performance.

SELECT
    COALESCE(a.agency_code, f.agency_code) AS agency_code,
    COALESCE(c.complaint_type, f.complaint_type) AS complaint_type,
    COUNT(*) AS closed_requests,
    AVG(CASE WHEN f.resolution_time_hours >= 0 THEN f.resolution_time_hours END) AS avg_resolution_time_hours
FROM gold.fact_service_requests AS f
LEFT JOIN gold.dim_agency AS a
    ON f.agency_sk = a.agency_sk
LEFT JOIN gold.dim_complaint_type AS c
    ON f.complaint_type_sk = c.complaint_type_sk
WHERE f.is_closed = TRUE
GROUP BY
    COALESCE(a.agency_code, f.agency_code),
    COALESCE(c.complaint_type, f.complaint_type)
ORDER BY agency_code, complaint_type;
