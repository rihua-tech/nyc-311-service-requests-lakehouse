-- Starter reconciliation queries across bronze, silver, and gold.

WITH layer_counts AS (
    SELECT 'bronze' AS layer, COUNT(*) AS row_count
    FROM bronze.nyc311_service_requests_raw

    UNION ALL

    SELECT 'silver' AS layer, COUNT(*) AS row_count
    FROM silver.service_requests_clean

    UNION ALL

    SELECT 'gold_fact' AS layer, COUNT(*) AS row_count
    FROM gold.fact_service_requests
)
SELECT
    layer,
    row_count
FROM layer_counts
ORDER BY layer;

WITH distinct_request_counts AS (
    SELECT
        COUNT(DISTINCT source_record_id) AS bronze_distinct_request_ids
    FROM bronze.nyc311_service_requests_raw
),
silver_request_counts AS (
    SELECT
        COUNT(DISTINCT request_id) AS silver_distinct_request_ids
    FROM silver.service_requests_clean
)
SELECT
    bronze_distinct_request_ids,
    silver_distinct_request_ids,
    silver_distinct_request_ids - bronze_distinct_request_ids AS distinct_id_delta
FROM distinct_request_counts
CROSS JOIN silver_request_counts;

SELECT
    b.source_record_id,
    b.ingest_date
FROM bronze.nyc311_service_requests_raw AS b
LEFT JOIN silver.service_requests_clean AS s
    ON b.source_record_id = s.request_id
WHERE b.source_record_id IS NOT NULL
  AND s.request_id IS NULL
LIMIT 50;

-- TODO: Add partition-aware reconciliation once silver and gold write patterns are finalized.
