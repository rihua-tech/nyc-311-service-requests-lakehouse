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
    "ingestion_mode": "api_extract",
    "raw_landing_path": "",
    "batch_id": "",
    "window_start": "",
    "window_end": "",
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


def _first_non_blank(*values: Any) -> str | None:
    for value in values:
        resolved = str(value).strip() if value is not None else ""
        if resolved:
            return resolved
    return None


def quote_identifier(identifier: str) -> str:
    """Safely quote a catalog, schema, or table identifier for Spark SQL."""
    return f"`{identifier.replace('`', '``')}`"


def _is_missing_external_location_error(exc: Exception) -> bool:
    message = str(exc)
    return "NO_PARENT_EXTERNAL_LOCATION_FOR_PATH" in message or "external location" in message.lower()


def _is_unavailable_storage_config_error(exc: Exception) -> bool:
    message = str(exc)
    return "CONFIG_NOT_AVAILABLE" in message and "fs.azure.account.auth.type" in message


def _normalize_path(path: str) -> str:
    return path.rstrip("/")


def _escape_sql_string(value: str) -> str:
    return value.replace("'", "''")


def _row_value(row: Any, key: str) -> Any:
    if isinstance(row, Mapping):
        return row[key]
    try:
        return row[key]
    except Exception:
        return getattr(row, key)


def infer_unambiguous_catalog(available_catalogs: Sequence[str]) -> str | None:
    """Choose the single non-system catalog when the workspace exposes exactly one."""
    excluded_catalogs = {"samples", "system", "hive_metastore", "spark_catalog"}
    candidates = [catalog for catalog in available_catalogs if catalog not in excluded_catalogs]
    return candidates[0] if len(candidates) == 1 else None


def _resolve_storage_filesystems(azure: Mapping[str, Any]) -> tuple[str, str]:
    shared_filesystem = _first_non_blank(azure.get("container"), azure.get("filesystem"))
    raw_filesystem = _first_non_blank(
        azure.get("raw_container"),
        azure.get("raw_filesystem"),
        shared_filesystem,
        azure.get("curated_container"),
        azure.get("curated_filesystem"),
    )
    curated_filesystem = _first_non_blank(
        azure.get("curated_container"),
        azure.get("curated_filesystem"),
        shared_filesystem,
        raw_filesystem,
    )

    if not raw_filesystem or not curated_filesystem:
        raise ValueError(
            "Environment config must define azure.storage_account and either a shared "
            "azure.container/filesystem or raw and curated container/filesystem values."
        )

    return raw_filesystem, curated_filesystem


def _apply_widget_overrides(config: dict[str, Any], overrides: Mapping[str, Any] | None = None) -> dict[str, Any]:
    resolved = deepcopy(config)
    if not overrides:
        return resolved

    source = resolved.setdefault("source", {})
    databricks = resolved.setdefault("databricks", {})
    runtime = resolved.setdefault("runtime", {})

    if overrides.get("page_size") is not None:
        source["page_size"] = int(overrides["page_size"])
    if overrides.get("max_pages_per_run") is not None:
        source["max_pages_per_run"] = int(overrides["max_pages_per_run"])
    if overrides.get("run_date") is not None:
        runtime["run_date"] = str(overrides["run_date"]).strip()
    if overrides.get("ingestion_mode") is not None:
        runtime["ingestion_mode"] = str(overrides["ingestion_mode"]).strip()
    if overrides.get("raw_landing_path") is not None:
        runtime["raw_landing_path"] = str(overrides["raw_landing_path"]).strip()
    if overrides.get("batch_id") is not None:
        runtime["batch_id"] = str(overrides["batch_id"]).strip()
    if overrides.get("window_start") is not None:
        runtime["window_start"] = str(overrides["window_start"]).strip()
    if overrides.get("window_end") is not None:
        runtime["window_end"] = str(overrides["window_end"]).strip()
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
    runtime = config.setdefault("runtime", {})
    source = config.setdefault("source", {})
    paths = config.setdefault("paths", {})

    storage_account = azure.get("storage_account")
    raw_filesystem, curated_filesystem = _resolve_storage_filesystems(azure)
    if not storage_account:
        raise ValueError(
            "Environment config must define azure.storage_account and either a shared "
            "azure.container/filesystem or raw and curated container/filesystem values."
        )

    bronze_base_path = paths.get("bronze_base_path") or build_abfss_uri(curated_filesystem, storage_account, "bronze")
    silver_base_path = paths.get("silver_base_path") or build_abfss_uri(curated_filesystem, storage_account, "silver")
    gold_base_path = paths.get("gold_base_path") or build_abfss_uri(curated_filesystem, storage_account, "gold")
    checkpoint_base_path = paths.get("checkpoint_base_path") or build_abfss_uri(
        raw_filesystem,
        storage_account,
        "bronze",
        "checkpoints",
    )
    bronze_raw_batch_path = paths.get("bronze_raw_batch_path") or build_abfss_uri(
        raw_filesystem,
        storage_account,
        "bronze",
        "nyc311_service_requests_raw",
        "raw_batches",
    )

    watermark_state_path = source.get("watermark_state_path")
    if not watermark_state_path or str(watermark_state_path).startswith("local_state/"):
        watermark_state_path = build_abfss_uri(
            raw_filesystem,
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

    azure["raw_container"] = raw_filesystem
    azure["curated_container"] = curated_filesystem
    azure["container"] = _first_non_blank(azure.get("container"), azure.get("filesystem"), curated_filesystem)
    databricks.setdefault("secret_scope", "adls-sp")
    databricks["catalog"] = str(databricks.get("catalog") or "").strip()
    runtime["run_date"] = str(runtime.get("run_date") or "").strip()
    runtime["ingestion_mode"] = str(runtime.get("ingestion_mode") or "api_extract").strip() or "api_extract"
    runtime["raw_landing_path"] = str(runtime.get("raw_landing_path") or "").strip()
    runtime["batch_id"] = str(runtime.get("batch_id") or "").strip()
    runtime["window_start"] = str(runtime.get("window_start") or "").strip()
    runtime["window_end"] = str(runtime.get("window_end") or "").strip()
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

    account_fqdn = f"{storage_account}.dfs.core.windows.net"
    try:
        spark.conf.set(f"fs.azure.account.auth.type.{account_fqdn}", "OAuth")
    except Exception as exc:
        if not _is_unavailable_storage_config_error(exc):
            raise
        # Serverless / Spark Connect can block direct fs.azure Spark configs. In that case
        # the notebooks rely on Unity Catalog + external locations instead of manual ABFS auth.
        print(
            f"Spark storage config is not available for {account_fqdn}; "
            "continuing with workspace-managed access."
        )
        return

    client_id = dbutils.secrets.get(scope=secret_scope, key=client_id_key)
    client_secret = dbutils.secrets.get(scope=secret_scope, key=client_secret_key)
    tenant_id = dbutils.secrets.get(scope=secret_scope, key=tenant_id_key)

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
        inferred_catalog = infer_unambiguous_catalog(available_catalogs)
        if inferred_catalog:
            catalog = inferred_catalog
            if isinstance(databricks, dict):
                databricks["catalog"] = catalog

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
    """Persist Delta files to the configured path and ensure the UC table points to that same location."""

    def build_writer() -> Any:
        writer = dataframe.write.format("delta").mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        if mode == "overwrite":
            writer = writer.option("overwriteSchema", "true")
        return writer

    build_writer().save(table_path)

    escaped_path = _escape_sql_string(table_path)
    spark = dataframe.sparkSession
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{escaped_path}'")

    detail_rows = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()
    if not detail_rows:
        raise ValueError(f"Unable to validate the registered location for {table_name}.")

    registered_location = str(_row_value(detail_rows[0], "location"))
    if _normalize_path(registered_location) != _normalize_path(table_path):
        raise ValueError(
            f"{table_name} is registered at {registered_location}, expected {table_path}. "
            "Drop and recreate the table registration or correct the configured table path."
        )


def bootstrap_notebook(
    spark: Any,
    dbutils: Any,
    extra_widget_defaults: Mapping[str, str] | None = None,
    ensure_catalog_schemas: bool = True,
    configure_storage_access: bool = True,
) -> dict[str, Any]:
    """Create widgets, resolve runtime config, optionally configure ADLS, and ensure schemas."""
    widget_defaults = dict(BASE_WIDGET_DEFAULTS)
    if extra_widget_defaults:
        widget_defaults.update(extra_widget_defaults)

    ensure_text_widgets(dbutils, widget_defaults)

    environment = get_widget(dbutils, "environment", default="dev")
    overrides = {
        "catalog": get_widget(dbutils, "catalog", default=""),
        "run_date": get_widget(dbutils, "run_date", default=""),
        "ingestion_mode": get_widget(dbutils, "ingestion_mode", default="api_extract"),
        "raw_landing_path": get_widget(dbutils, "raw_landing_path", default=""),
        "batch_id": get_widget(dbutils, "batch_id", default=""),
        "window_start": get_widget(dbutils, "window_start", default=""),
        "window_end": get_widget(dbutils, "window_end", default=""),
        "secret_scope": get_widget(dbutils, "secret_scope", default=""),
        "page_size": get_widget_int(dbutils, "page_size"),
        "max_pages_per_run": get_widget_int(dbutils, "max_pages_per_run"),
    }
    config = resolve_runtime_config(environment, overrides=overrides)
    databricks = config["databricks"]

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    if configure_storage_access:
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
