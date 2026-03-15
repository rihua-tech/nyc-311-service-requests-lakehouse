-- Starter SQL checks for required-field nulls across bronze and silver.

SELECT 'bronze.nyc311_service_requests_raw' AS table_name, 'raw_payload' AS column_name, COUNT(*) AS null_or_blank_rows
FROM bronze.nyc311_service_requests_raw
WHERE raw_payload IS NULL OR TRIM(raw_payload) = ''

UNION ALL

SELECT 'bronze.nyc311_service_requests_raw' AS table_name, 'record_hash' AS column_name, COUNT(*) AS null_or_blank_rows
FROM bronze.nyc311_service_requests_raw
WHERE record_hash IS NULL OR TRIM(record_hash) = ''

UNION ALL

SELECT 'silver.service_requests_clean' AS table_name, 'request_id' AS column_name, COUNT(*) AS null_or_blank_rows
FROM silver.service_requests_clean
WHERE request_id IS NULL OR TRIM(request_id) = ''

UNION ALL

SELECT 'silver.service_requests_clean' AS table_name, 'created_at' AS column_name, COUNT(*) AS null_or_blank_rows
FROM silver.service_requests_clean
WHERE created_at IS NULL

UNION ALL

SELECT 'silver.service_requests_clean' AS table_name, 'created_date' AS column_name, COUNT(*) AS null_or_blank_rows
FROM silver.service_requests_clean
WHERE created_date IS NULL

UNION ALL

SELECT 'silver.service_requests_clean' AS table_name, 'record_hash' AS column_name, COUNT(*) AS null_or_blank_rows
FROM silver.service_requests_clean
WHERE record_hash IS NULL OR TRIM(record_hash) = '';

-- TODO: Expand with layer-specific thresholds once production expectations exist.
