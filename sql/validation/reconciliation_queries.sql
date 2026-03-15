-- Starter reconciliation queries across medallion layers.

SELECT 'bronze' AS layer, COUNT(*) AS row_count
FROM bronze.nyc311_service_requests_raw

UNION ALL

SELECT 'silver' AS layer, COUNT(*) AS row_count
FROM silver.service_requests_clean

UNION ALL

SELECT 'gold_fact' AS layer, COUNT(*) AS row_count
FROM gold.fact_service_requests;

-- TODO: Add partition-level reconciliation by ingest_date or created_date.
