# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Ingest NYC311 Raw
# MAGIC
# MAGIC Purpose:
# MAGIC - Load NYC 311 raw data into the bronze layer from either a Databricks-managed API extract
# MAGIC   or an ADF-landed raw JSON handoff.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - `environment`
# MAGIC - `catalog`
# MAGIC - `ingestion_mode`
# MAGIC - `raw_landing_path` when ADF lands raw JSON before Databricks
# MAGIC - `batch_id`, `run_date`, `window_start`, and `window_end` for lineage and handoff tracing
# MAGIC - optional `watermark_value` when Databricks still owns API extraction
# MAGIC
# MAGIC Expected outputs:
# MAGIC - writes to `bronze.nyc311_service_requests_raw`
# MAGIC - updates the source watermark only when Databricks owns API extraction; in
# MAGIC   `adf_landed_raw` mode ADF owns the source extraction window
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Load config.
# MAGIC 2. Read landed raw data.
# MAGIC 3. Attach ingest metadata.
# MAGIC 4. Write Delta bronze rows.

# COMMAND ----------
import importlib

from pyspark.sql import functions as F

from src.common.constants import BRONZE_TABLE
from src.common import databricks_runtime as runtime
from src.common.utils import utc_now_iso
from src.ingestion.api_extract import extract_service_requests
from src.ingestion.bronze_loader import prepare_bronze_batch
from src.ingestion.watermark import build_watermark_state, get_latest_watermark

runtime = importlib.reload(runtime)


def read_current_watermark(path: str) -> str | None:
    try:
        watermark_df = spark.read.format("delta").load(path)  # type: ignore[name-defined]
        rows = watermark_df.orderBy("updated_at", ascending=False).limit(1).collect()  # type: ignore[name-defined]
    except Exception as exc:
        message = str(exc)
        if "PATH_NOT_FOUND" in message or "Path does not exist" in message:
            return None
        raise

    return str(rows[0]["value"]) if rows else None


def build_landed_raw_bronze_df(raw_landing_path: str, source_system: str):
    raw_df = spark.read.option("multiLine", "true").json(raw_landing_path)  # type: ignore[name-defined]
    if raw_df.columns:
        raw_payload_expr = F.to_json(F.struct(*[F.col(column_name) for column_name in raw_df.columns]))
    else:
        raw_payload_expr = F.lit("{}")

    source_record_expr = (
        F.col("unique_key").cast("string") if "unique_key" in raw_df.columns else F.lit(None).cast("string")
    )
    ingest_timestamp_expr = F.current_timestamp()

    return (
        raw_df.withColumn("_raw_payload", raw_payload_expr)
        .select(
            F.expr("uuid()").alias("ingest_id"),
            F.lit(source_system).alias("source_system"),
            F.coalesce(source_record_expr, F.lit("<missing-source-record-id>")).alias("source_record_id"),
            ingest_timestamp_expr.alias("ingest_timestamp"),
            F.to_date(ingest_timestamp_expr).alias("ingest_date"),
            ingest_timestamp_expr.alias("api_pull_timestamp"),
            F.col("_raw_payload").alias("raw_payload"),
            F.sha2(F.col("_raw_payload"), 256).alias("record_hash"),
            F.input_file_name().alias("file_path"),
        )
    )


def write_bronze_dataframe(bronze_df):
    write_mode = "append" if spark.catalog.tableExists(BRONZE_TABLE) else "overwrite"  # type: ignore[name-defined]
    runtime.write_delta_table(bronze_df, BRONZE_TABLE, table_path, mode=write_mode)


config = runtime.bootstrap_notebook(
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
runtime_config = config["runtime"]
table_path = paths["table_paths"][BRONZE_TABLE]
watermark_path = source_config["watermark_state_path"]
ingestion_mode = str(runtime_config.get("ingestion_mode") or "api_extract").strip() or "api_extract"
raw_landing_path = str(runtime_config.get("raw_landing_path") or "").strip()
batch_id = str(runtime_config.get("batch_id") or "").strip()
window_start = str(runtime_config.get("window_start") or "").strip()
window_end = str(runtime_config.get("window_end") or "").strip()

print(f"Ingestion mode: {ingestion_mode}")
if batch_id:
    print(f"Batch ID: {batch_id}")
if window_start or window_end:
    print(f"Source window: {window_start or '<unset>'} -> {window_end or '<unset>'}")

if ingestion_mode == "api_extract":
    watermark_override = runtime.get_widget(dbutils, "watermark_value", default="")  # type: ignore[name-defined]
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
            batch_id=batch_id or None,
        )
        bronze_df = spark.createDataFrame(bronze_rows)  # type: ignore[name-defined]
        write_bronze_dataframe(bronze_df)

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
elif ingestion_mode == "adf_landed_raw":
    if not raw_landing_path:
        raise ValueError("raw_landing_path is required when ingestion_mode='adf_landed_raw'.")

    landed_bronze_df = build_landed_raw_bronze_df(
        raw_landing_path=raw_landing_path,
        source_system=source_config.get("system_name", "nyc_311_api"),
    )
    landed_record_count = landed_bronze_df.count()

    if not landed_record_count:
        print(f"No landed raw records found at {raw_landing_path}.")
    else:
        write_bronze_dataframe(landed_bronze_df)
        print(f"Loaded {landed_record_count} landed raw records into {BRONZE_TABLE}")
        print(f"Bronze table path: {table_path}")
        print(f"Raw landing path: {raw_landing_path}")
        print("Watermark state was not updated because ADF owns source extraction in this mode.")
else:
    raise ValueError(
        f"Unsupported ingestion_mode={ingestion_mode!r}. "
        "Expected one of: 'api_extract', 'adf_landed_raw'."
    )
