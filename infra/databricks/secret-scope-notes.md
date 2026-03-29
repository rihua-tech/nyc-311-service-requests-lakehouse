# Secret Scope Notes

This document describes the current Milestone 9 secret-handling pattern used by the manual Databricks cloud run. It documents secret names and lookup behavior, not secret values.

## Current Expected Secret Scope Pattern

- current dev setup expects a Databricks secret scope named `adls-sp`
- current dev setup expects these secret keys:
  - `client-id`
  - `client-secret`
  - `tenant-id`
- the backing store for the scope is not committed to this repo

## How The Repo Uses Those Secrets

- `databricks/notebooks/00_setup/01_secrets_and_widgets.py` reads widget values for `secret_scope`, `sp_client_id_key`, `sp_client_secret_key`, and `sp_tenant_id_key`
- the setup notebook validates that those secret names can be resolved with `dbutils.secrets.get` without printing any secret values
- `src/common/databricks_runtime.py` uses the retrieved values to configure OAuth-based ADLS access when manual Spark storage configuration is required
- if the Databricks environment already exposes storage through workspace-managed access or external locations, the runtime can continue without hardcoded credentials

## Non-Secret Runtime Inputs

These values are configuration, not secrets:

- `environment`
- `run_date`
- `catalog`
- `page_size`
- `max_pages_per_run`
- `snapshot_date`
- `bronze_schema`
- `silver_schema`
- `gold_schema`
- `storage_account`
- `container`

## Optional Future Secrets

- `nyc311-api-token`
- `alert-webhook-url`
- `job-notification-email`

The current Milestone 9 run does not require those extras to prove the manual Databricks-to-ADLS path.

## Honest Status

- the secret lookup flow is real in the Databricks setup notebooks
- this repo does not contain live secret values, workspace tokens, or a production Key Vault deployment definition
- a more mature deployment may switch to Key Vault-backed scopes, managed identity, or Unity Catalog external-location access without changing the public repo documentation style
