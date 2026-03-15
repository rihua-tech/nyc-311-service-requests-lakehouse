-- Starter Databricks SQL checks for data quality review.

SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT request_id) AS distinct_request_ids
FROM silver.service_requests_clean;

SELECT
    COUNT(*) AS null_request_ids
FROM silver.service_requests_clean
WHERE request_id IS NULL;

-- TODO: Add threshold checks and exception outputs for operational monitoring.

