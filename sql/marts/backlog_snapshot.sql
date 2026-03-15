-- Starter mart template for backlog snapshot reporting.

SELECT
    CURRENT_DATE() AS snapshot_date,
    COALESCE(s.status_name, f.status_name, '<unknown>') AS status_name,
    COALESCE(a.agency_code, f.agency_code, '<unknown>') AS agency_code,
    COUNT(*) AS open_request_count
FROM gold.fact_service_requests AS f
LEFT JOIN gold.dim_status AS s
    ON f.status_sk = s.status_sk
LEFT JOIN gold.dim_agency AS a
    ON f.agency_sk = a.agency_sk
WHERE f.is_closed = FALSE
  AND f.created_date <= CURRENT_DATE()
  AND (f.closed_date IS NULL OR f.closed_date > CURRENT_DATE())
GROUP BY
    COALESCE(s.status_name, f.status_name, '<unknown>'),
    COALESCE(a.agency_code, f.agency_code, '<unknown>')
ORDER BY status_name, agency_code;
