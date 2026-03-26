# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Secrets And Widgets
# MAGIC
# MAGIC Purpose:
# MAGIC - Define notebook widgets and outline secret retrieval requirements.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - `environment`
# MAGIC - `run_date`
# MAGIC
# MAGIC Expected outputs:
# MAGIC - notebook parameters available to downstream tasks
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Create widgets.
# MAGIC 2. Read secrets from a secret scope.
# MAGIC 3. Validate required runtime values.

# COMMAND ----------
import importlib

from src.common import databricks_runtime as runtime

runtime = importlib.reload(runtime)

WIDGET_DEFAULTS = dict(runtime.BASE_WIDGET_DEFAULTS)
WIDGET_DEFAULTS.update(
    {
        "page_size": "",
        "max_pages_per_run": "",
        "snapshot_date": "",
    }
)

try:
    runtime.ensure_text_widgets(dbutils, WIDGET_DEFAULTS)  # type: ignore[name-defined]
    environment = runtime.get_widget(dbutils, "environment", default="dev")  # type: ignore[name-defined]
    config = runtime.resolve_runtime_config(
        environment,
        overrides={
            "run_date": runtime.get_widget(dbutils, "run_date", default=""),  # type: ignore[name-defined]
            "secret_scope": runtime.get_widget(dbutils, "secret_scope", default=""),  # type: ignore[name-defined]
            "page_size": runtime.get_widget_int(dbutils, "page_size"),  # type: ignore[name-defined]
            "max_pages_per_run": runtime.get_widget_int(dbutils, "max_pages_per_run"),  # type: ignore[name-defined]
        },
    )

    secret_scope = runtime.get_widget(  # type: ignore[name-defined]
        dbutils,
        "secret_scope",
        default=config["databricks"]["secret_scope"],
    )
    catalog = runtime.get_widget(  # type: ignore[name-defined]
        dbutils,
        "catalog",
        default=config["databricks"].get("catalog", ""),
    )
    client_id_key = runtime.get_widget(  # type: ignore[name-defined]
        dbutils,
        "sp_client_id_key",
        default=config["databricks"].get("sp_client_id_key", ""),
    )
    client_secret_key = runtime.get_widget(  # type: ignore[name-defined]
        dbutils,
        "sp_client_secret_key",
        default=config["databricks"].get("sp_client_secret_key", ""),
    )
    tenant_id_key = runtime.get_widget(  # type: ignore[name-defined]
        dbutils,
        "sp_tenant_id_key",
        default=config["databricks"].get("sp_tenant_id_key", ""),
    )

    print(f"Environment: {environment}")
    print(f"Secret scope: {secret_scope}")
    print(f"Storage account: {config['azure']['storage_account']}")
    print(f"Filesystem: {config['azure']['container']}")
    print(f"Configured catalog: {catalog or '<not set>'}")
    print(f"Bronze base path: {config['paths']['bronze_base_path']}")
    print(f"Silver base path: {config['paths']['silver_base_path']}")
    print(f"Gold base path: {config['paths']['gold_base_path']}")

    if not client_id_key or not client_secret_key or not tenant_id_key:
        # Manual step: set these widget values to the existing secret names in scope `adls-sp`.
        raise ValueError(
            "Populate sp_client_id_key, sp_client_secret_key, and sp_tenant_id_key with the names "
            "already stored in the Databricks secret scope before running the processing notebooks."
        )

    # Validate that the configured secret names can be resolved without printing any secret values.
    dbutils.secrets.get(scope=secret_scope, key=client_id_key)  # type: ignore[name-defined]
    dbutils.secrets.get(scope=secret_scope, key=client_secret_key)  # type: ignore[name-defined]
    dbutils.secrets.get(scope=secret_scope, key=tenant_id_key)  # type: ignore[name-defined]
    print("Secret lookups validated successfully.")

    resolved_catalog = runtime.validate_catalog_access(spark, config)  # type: ignore[name-defined]
    accessible_catalogs = [str(row[0]) for row in spark.sql("SHOW CATALOGS").collect()]  # type: ignore[name-defined]
    print(f"Validated catalog: {resolved_catalog}")
    print(f"Accessible catalogs in this session: {', '.join(sorted(accessible_catalogs))}")
except NameError:
    print("This notebook is intended to run inside Databricks.")
