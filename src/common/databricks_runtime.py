"""Helpers for Databricks notebook runtime configuration."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Mapping, Sequence

from src.common.config_loader import load_environment_config
from src.common.constants import BRONZE_TABLE, GOLD_TABLES, SILVER_REFERENCE_TABLES, SILVER_TABLE

BASE_WIDGET_DEFAULTS = {
    "environment": "dev",
    "catalog": "",
    "run_date": "",
    "secret_scope": "",
    "sp_client_id_key": "",
    "sp_client_secret_key": "",
    "sp_tenant_id_key": "",
}


def build_abfss_uri(filesystem: str, storage_account: str, *parts: str) -> str:
    """Build an ABFSS URI from a filesystem, storage account, and path parts."""
    base_uri = f"abfss://{filesystem}@{storage_account}.dfs.core.windows.net"
    clean_parts = [part.strip("/") for part in parts if part]
    if not clean_parts:
        return base_uri
    return f"{base_uri}/{'/'.join(clean_parts)}"


def _table_path(base_path: str, table_name: str) -> str:
    return f"{base_path.rstrip('/')}/{table_name.split('.')[-1]}"


def quote_identifier(identifier: str) -> str:
    """Safely quote a catalog, schema, or table identifier for Spark SQL."""
    return f"`{identifier.replace('`', '``')}`"


def _apply_widget_overrides(config: dict[str, Any], overrides: Mapping[str, Any] | None = None) -> dict[str, Any]:
    resolved = deepcopy(config)
    if not overrides:
        return resolved

    source = resolved.setdefault("source", {})
    databricks = resolved.setdefault("databricks", {})

    if overrides.get("page_size") is not None:
        source["page_size"] = int(overrides["page_size"])
    if overrides.get("max_pages_per_run") is not None:
        source["max_pages_per_run"] = int(overrides["max_pages_per_run"])
    if overrides.get("run_date") is not None:
        resolved.setdefault("runtime", {})["run_date"] = overrides["run_date"]
    if overrides.get("secret_scope"):
        databricks["secret_scope"] = str(overrides["secret_scope"])
    if overrides.get("catalog") is not None:
        databricks["catalog"] = str(overrides["catalog"]).strip()

    return resolved


def resolve_runtime_config(
    environment: str,
    overrides: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Load environment config and derive notebook runtime paths."""
    config = _apply_widget_overrides(load_environment_config(environment), overrides=overrides)

    azure = config.setdefault("azure", {})
    databricks = config.setdefault("databricks", {})
    source = config.setdefault("source", {})
    paths = config.setdefault("paths", {})

    storage_account = azure.get("storage_account")
    filesystem = azure.get("container") or azure.get("filesystem") or azure.get("raw_container") or azure.get("curated_container")
    if not storage_account or not filesystem:
        raise ValueError("Environment config must define azure.storage_account and azure.container/filesystem.")

    bronze_base_path = paths.get("bronze_base_path") or build_abfss_uri(filesystem, storage_account, "bronze")
    silver_base_path = paths.get("silver_base_path") or build_abfss_uri(filesystem, storage_account, "silver")
    gold_base_path = paths.get("gold_base_path") or build_abfss_uri(filesystem, storage_account, "gold")
    checkpoint_base_path = paths.get("checkpoint_base_path") or build_abfss_uri(
        filesystem,
        storage_account,
        "bronze",
        "checkpoints",
    )
    bronze_raw_batch_path = paths.get("bronze_raw_batch_path") or build_abfss_uri(
        filesystem,
        storage_account,
        "bronze",
        "nyc311_service_requests_raw",
        "raw_batches",
    )

    watermark_state_path = source.get("watermark_state_path")
    if not watermark_state_path or str(watermark_state_path).startswith("local_state/"):
        watermark_state_path = build_abfss_uri(
            filesystem,
            storage_account,
            "bronze",
            "checkpoints",
            "nyc311_service_requests",
            "watermark_state",
        )

    table_paths = {
        BRONZE_TABLE: _table_path(bronze_base_path, BRONZE_TABLE),
        SILVER_TABLE: _table_path(silver_base_path, SILVER_TABLE),
    }
    table_paths.update(
        {
            table_name: _table_path(silver_base_path, table_name)
            for table_name in SILVER_REFERENCE_TABLES.values()
        }
    )
    table_paths.update(
        {
            table_name: _table_path(gold_base_path, table_name)
            for table_name in GOLD_TABLES.values()
        }
    )

    azure["container"] = filesystem
    databricks.setdefault("secret_scope", "adls-sp")
    databricks["catalog"] = str(databricks.get("catalog") or "").strip()
    paths.update(
        {
            "bronze_base_path": bronze_base_path,
            "silver_base_path": silver_base_path,
            "gold_base_path": gold_base_path,
            "checkpoint_base_path": checkpoint_base_path,
            "bronze_raw_batch_path": bronze_raw_batch_path,
            "table_paths": table_paths,
        }
    )
    source["watermark_state_path"] = watermark_state_path

    return config


def ensure_text_widgets(dbutils: Any, widget_defaults: Mapping[str, str]) -> None:
    """Create text widgets when they do not already exist."""
    for name, default in widget_defaults.items():
        try:
            dbutils.widgets.get(name)
        except Exception:
            dbutils.widgets.text(name, default)


def get_widget(dbutils: Any, name: str, default: str = "") -> str:
    """Read a Databricks widget value when available."""
    try:
        value = dbutils.widgets.get(name)
    except Exception:
        return default
    return value if value not in (None, "") else default


def get_widget_int(dbutils: Any, name: str) -> int | None:
    """Read an optional integer widget value."""
    raw_value = get_widget(dbutils, name, default="")
    return int(raw_value) if raw_value else None


def configure_adls_service_principal_access(
    spark: Any,
    dbutils: Any,
    storage_account: str,
    secret_scope: str,
    client_id_key: str,
    client_secret_key: str,
    tenant_id_key: str,
) -> None:
    """Configure Spark to access ADLS Gen2 via a Databricks secret scope."""
    if not secret_scope:
        raise ValueError("A secret scope is required to configure ADLS access.")
    if not client_id_key or not client_secret_key or not tenant_id_key:
        raise ValueError(
            "Populate the widget values for sp_client_id_key, sp_client_secret_key, and sp_tenant_id_key "
            "before running the setup or processing notebooks."
        )

    client_id = dbutils.secrets.get(scope=secret_scope, key=client_id_key)
    client_secret = dbutils.secrets.get(scope=secret_scope, key=client_secret_key)
    tenant_id = dbutils.secrets.get(scope=secret_scope, key=tenant_id_key)

    account_fqdn = f"{storage_account}.dfs.core.windows.net"
    spark.conf.set(f"fs.azure.account.auth.type.{account_fqdn}", "OAuth")
    spark.conf.set(
        f"fs.azure.account.oauth.provider.type.{account_fqdn}",
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    )
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{account_fqdn}", client_id)
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{account_fqdn}", client_secret)
    spark.conf.set(
        f"fs.azure.account.oauth2.client.endpoint.{account_fqdn}",
        f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
    )


def validate_catalog_access(spark: Any, config: Mapping[str, Any]) -> str:
    """Validate that the configured Unity Catalog catalog exists and is accessible."""
    databricks = config.get("databricks", {})
    environment = str(config.get("environment", "<unknown>"))
    catalog = str(databricks.get("catalog") or "").strip()

    available_catalogs: list[str] = []
    try:
        available_catalogs = [str(row[0]) for row in spark.sql("SHOW CATALOGS").collect()]
    except Exception:
        available_catalogs = []

    if not catalog:
        visible_catalogs = ", ".join(sorted(available_catalogs)) if available_catalogs else "<none returned>"
        raise ValueError(
            "No Unity Catalog catalog is configured. Set databricks.catalog in "
            f"config/{environment}.yaml or notebook widget 'catalog' to the catalog you can use for this workspace. "
            f"Visible catalogs from this session: {visible_catalogs}"
        )

    if available_catalogs and catalog not in available_catalogs:
        visible_catalogs = ", ".join(sorted(available_catalogs))
        raise ValueError(
            f"Configured databricks.catalog='{catalog}' is not visible to this workspace principal or cluster. "
            f"Set databricks.catalog in config/{environment}.yaml or notebook widget 'catalog' to a Unity Catalog "
            f"catalog you can actually USE in this workspace. Visible catalogs: {visible_catalogs}"
        )

    try:
        spark.sql(f"USE CATALOG {quote_identifier(catalog)}")
    except Exception as exc:
        raise ValueError(
            f"Configured databricks.catalog='{catalog}' could not be activated in this Unity Catalog workspace. "
            f"Set databricks.catalog in config/{environment}.yaml or notebook widget 'catalog' to a catalog "
            "that exists and is accessible to the current Databricks user/cluster."
        ) from exc

    return catalog


def ensure_catalog_and_schemas(spark: Any, config: Mapping[str, Any]) -> None:
    """Set the active catalog and create the bronze, silver, and gold schemas."""
    databricks = config.get("databricks", {})
    catalog = validate_catalog_access(spark, config)

    for schema_name in (
        databricks.get("bronze_schema", "bronze"),
        databricks.get("silver_schema", "silver"),
        databricks.get("gold_schema", "gold"),
    ):
        if schema_name:
            spark.sql(
                f"CREATE SCHEMA IF NOT EXISTS {quote_identifier(catalog)}.{quote_identifier(str(schema_name))}"
            )


def write_delta_table(
    dataframe: Any,
    table_name: str,
    table_path: str,
    mode: str = "overwrite",
    partition_by: Sequence[str] | None = None,
) -> None:
    """Persist a DataFrame to a Delta table with an explicit ADLS-backed path."""
    writer = dataframe.write.format("delta").mode(mode).option("path", table_path)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    writer.saveAsTable(table_name)


def bootstrap_notebook(
    spark: Any,
    dbutils: Any,
    extra_widget_defaults: Mapping[str, str] | None = None,
    ensure_catalog_schemas: bool = True,
) -> dict[str, Any]:
    """Create widgets, resolve runtime config, configure ADLS, and ensure schemas."""
    widget_defaults = dict(BASE_WIDGET_DEFAULTS)
    if extra_widget_defaults:
        widget_defaults.update(extra_widget_defaults)

    ensure_text_widgets(dbutils, widget_defaults)

    environment = get_widget(dbutils, "environment", default="dev")
    overrides = {
        "catalog": get_widget(dbutils, "catalog", default=""),
        "run_date": get_widget(dbutils, "run_date", default=""),
        "secret_scope": get_widget(dbutils, "secret_scope", default=""),
        "page_size": get_widget_int(dbutils, "page_size"),
        "max_pages_per_run": get_widget_int(dbutils, "max_pages_per_run"),
    }
    config = resolve_runtime_config(environment, overrides=overrides)
    databricks = config["databricks"]

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    configure_adls_service_principal_access(
        spark=spark,
        dbutils=dbutils,
        storage_account=config["azure"]["storage_account"],
        secret_scope=get_widget(dbutils, "secret_scope", default=databricks["secret_scope"]),
        client_id_key=get_widget(dbutils, "sp_client_id_key", default=databricks.get("sp_client_id_key", "")),
        client_secret_key=get_widget(
            dbutils,
            "sp_client_secret_key",
            default=databricks.get("sp_client_secret_key", ""),
        ),
        tenant_id_key=get_widget(dbutils, "sp_tenant_id_key", default=databricks.get("sp_tenant_id_key", "")),
    )
    if ensure_catalog_schemas:
        ensure_catalog_and_schemas(spark, config)

    return config
