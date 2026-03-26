# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Ingest NYC311 Raw
# MAGIC
# MAGIC Purpose:
# MAGIC - Load raw NYC 311 API data into the bronze layer.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - `environment`
# MAGIC - raw landing path or API extraction output
# MAGIC - optional watermark value
# MAGIC
# MAGIC Expected outputs:
# MAGIC - writes to `bronze.nyc311_service_requests_raw`
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Load config.
# MAGIC 2. Read landed raw data.
# MAGIC 3. Attach ingest metadata.
# MAGIC 4. Write Delta bronze rows.

# COMMAND ----------
from pyspark.sql import functions as F

from src.common.constants import BRONZE_TABLE
from src.common.databricks_runtime import bootstrap_notebook, get_widget, write_delta_table
from src.common.utils import utc_now_iso
from src.ingestion.api_extract import extract_service_requests
from src.ingestion.bronze_loader import prepare_bronze_batch
from src.ingestion.watermark import build_watermark_state, get_latest_watermark


def read_current_watermark(path: str) -> str | None:
    try:
        watermark_df = spark.read.format("delta").load(path)  # type: ignore[name-defined]
    except Exception:
        return None

    rows = watermark_df.orderBy(F.col("updated_at").desc()).limit(1).collect()
    return str(rows[0]["value"]) if rows else None


config = bootstrap_notebook(
    spark=spark,  # type: ignore[name-defined]
    dbutils=dbutils,  # type: ignore[name-defined]
    extra_widget_defaults={
        "page_size": "",
        "max_pages_per_run": "",
        "watermark_value": "",
    },
)

source_config = config["source"]
paths = config["paths"]
table_path = paths["table_paths"][BRONZE_TABLE]
watermark_path = source_config["watermark_state_path"]

watermark_override = get_widget(dbutils, "watermark_value", default="")  # type: ignore[name-defined]
resolved_watermark = watermark_override or read_current_watermark(watermark_path)
api_pull_timestamp = utc_now_iso()

records = extract_service_requests(
    base_url=source_config["base_url"],
    page_size=int(source_config["page_size"]),
    watermark_field=source_config["watermark_field"],
    watermark_value=resolved_watermark,
    max_pages=source_config.get("max_pages_per_run"),
    timeout=int(source_config.get("timeout_seconds", 30)),
    order_by=source_config.get("order_by"),
)

if not records:
    print(
        "No new records returned from the API. "
        f"Current watermark remains {resolved_watermark!r}."
    )
else:
    bronze_rows = prepare_bronze_batch(
        records=records,
        bronze_base_path=paths["bronze_raw_batch_path"],
        api_pull_timestamp=api_pull_timestamp,
        source_system=source_config.get("system_name", "nyc_311_api"),
    )
    bronze_df = spark.createDataFrame(bronze_rows)  # type: ignore[name-defined]
    write_mode = "append" if spark.catalog.tableExists(BRONZE_TABLE) else "overwrite"  # type: ignore[name-defined]
    write_delta_table(bronze_df, BRONZE_TABLE, table_path, mode=write_mode)

    latest_watermark = get_latest_watermark(
        record.get(source_config["watermark_field"]) for record in records
    )
    if latest_watermark:
        watermark_state = build_watermark_state(
            column_name=source_config["watermark_field"],
            value=latest_watermark,
        )
        spark.createDataFrame([watermark_state]).write.format("delta").mode("overwrite").save(  # type: ignore[name-defined]
            watermark_path
        )

    print(f"Loaded {len(records)} source records into {BRONZE_TABLE}")
    print(f"Bronze table path: {table_path}")
    print(f"Watermark path: {watermark_path}")
    print(f"New watermark value: {latest_watermark}")
