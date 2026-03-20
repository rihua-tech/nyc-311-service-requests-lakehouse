# Secret Scope Notes

Use this document to track how secrets and runtime configuration should be exposed once the Databricks scaffold moves beyond local placeholders. This repo does not store live secret values, workspace tokens, or deployed scope bindings.

## Suggested Scope Pattern

- Preferred for Azure deployments: Azure Key Vault-backed scope such as `kv-nyc311-<env>`
- Acceptable placeholder pattern for early testing: Databricks secret scope such as `nyc311-<env>`
- Keep secret names stable across environments and swap the backing scope or vault per environment

## Secrets That May Be Needed

### Storage Authentication

Use these only if the workspace is not already using managed identity, Unity Catalog external locations, or another storage abstraction that removes the need for notebook-level secrets.

- `adls-client-id`
- `adls-client-secret`
- `adls-tenant-id`

### Source Access

- `nyc311-api-token`

The NYC 311 API can often be queried anonymously, but an app token is a believable production placeholder for rate-limit resilience and operational consistency.

### Optional Operational Integrations

- `alert-webhook-url`
- `job-notification-email`

## Non-Secret Runtime Configuration

These values are configuration, not secrets, and should be passed through job parameters, widgets, or environment-specific config files instead of a secret scope.

- `environment`
- `run_date`
- `catalog`
- `bronze_schema`
- `silver_schema`
- `gold_schema`
- `raw_landing_path`
- `checkpoint_path`
- `storage_account`
- `raw_container`
- `curated_container`

## Intended Usage In This Repo

- `databricks/notebooks/00_setup/01_secrets_and_widgets.py` is the natural place to centralize widget setup and secret retrieval once the notebooks move beyond scaffold status.
- Bronze ingestion would use runtime path settings and may optionally use `nyc311-api-token` if the chosen API access pattern requires it.
- Storage credentials should never be hardcoded in notebooks, workflow JSON, or checked-in config files.

## TODO

- decide between managed identity or Unity Catalog access versus service-principal-based storage access
- document the exact secret lookups once notebook parameters and cluster policies are finalized
- keep environment names, vault names, and scope names aligned with the Azure naming pattern used elsewhere in `infra/`
- avoid hardcoding any values in source control
