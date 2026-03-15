-- Starter SQL checks for required-field nulls.

SELECT COUNT(*) AS null_request_ids
FROM silver.service_requests_clean
WHERE request_id IS NULL;

SELECT COUNT(*) AS null_created_dates
FROM silver.service_requests_clean
WHERE created_date IS NULL;

-- TODO: Add checks for agency, complaint type, and status once rules are finalized.

