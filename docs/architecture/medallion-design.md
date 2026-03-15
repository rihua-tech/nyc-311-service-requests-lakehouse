# Medallion Design

## Bronze

- Table: `bronze.nyc311_service_requests_raw`
- Grain: one ingested source record per load event
- Contents: raw JSON payload plus ingestion metadata and lineage fields

## Silver

- Table: `silver.service_requests_clean`
- Grain: one standardized service request record
- Supporting tables:
  - `silver.agency_reference`
  - `silver.complaint_type_reference`
  - `silver.location_reference`
  - `silver.status_reference`

## Gold

- Dimensions support consistent slicing in Power BI
- Fact and mart tables support operational reporting use cases
- Business rules are intentionally not finalized in this scaffold

## TODO

- define surrogate key strategy for dimensions
- decide whether backlog snapshot is fully materialized or view-based
- document slowly changing dimension needs if the project evolves beyond starter scope

