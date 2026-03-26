# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Clean Service Requests
# MAGIC
# MAGIC Purpose:
# MAGIC - Standardize core service request fields from bronze to silver.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - `bronze.nyc311_service_requests_raw`
# MAGIC - bronze rows with `raw_payload`, `record_hash`, and ingest metadata
# MAGIC
# MAGIC Expected outputs:
# MAGIC - starter rows for `silver.service_requests_clean`
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Read bronze rows from Delta.
# MAGIC 2. Parse `raw_payload` and select core service request fields.
# MAGIC 3. Cast timestamps, normalize borough values, and derive operational fields.
# MAGIC 4. Remove duplicate `request_id` values and write silver output.

# COMMAND ----------
from pyspark.sql import Window
from pyspark.sql import functions as F

from src.common.constants import BRONZE_TABLE, SILVER_TABLE
from src.common.databricks_runtime import bootstrap_notebook, write_delta_table
from src.transformation.silver_service_requests import BOROUGH_NORMALIZATION_MAP


def clean_text(column: F.Column) -> F.Column:
    trimmed = F.trim(column)
    return F.when(column.isNull() | (trimmed == ""), F.lit(None)).otherwise(trimmed)


def parse_timestamp(column: F.Column) -> F.Column:
    return F.to_timestamp(clean_text(column))


def parse_double(column: F.Column) -> F.Column:
    cleaned = clean_text(column)
    return F.when(cleaned.isNull(), F.lit(None)).otherwise(cleaned.cast("double"))


config = bootstrap_notebook(spark=spark, dbutils=dbutils)  # type: ignore[name-defined]
table_path = config["paths"]["table_paths"][SILVER_TABLE]

if not spark.catalog.tableExists(BRONZE_TABLE):  # type: ignore[name-defined]
    raise ValueError(f"{BRONZE_TABLE} does not exist. Run the bronze notebooks first.")

bronze_df = spark.table(BRONZE_TABLE)  # type: ignore[name-defined]
borough_map = F.create_map([F.lit(item) for pair in BOROUGH_NORMALIZATION_MAP.items() for item in pair])

selected_df = bronze_df.select(
    F.coalesce(
        clean_text(F.get_json_object(F.col("raw_payload"), "$.unique_key")),
        clean_text(F.col("source_record_id")),
    ).alias("request_id"),
    parse_timestamp(F.get_json_object(F.col("raw_payload"), "$.created_date")).alias("created_at"),
    parse_timestamp(F.get_json_object(F.col("raw_payload"), "$.closed_date")).alias("closed_at"),
    parse_timestamp(
        F.coalesce(
            F.get_json_object(F.col("raw_payload"), "$.resolution_action_updated_date"),
            F.get_json_object(F.col("raw_payload"), "$.updated_date"),
        )
    ).alias("updated_at"),
    parse_timestamp(F.get_json_object(F.col("raw_payload"), "$.due_date")).alias("due_date"),
    clean_text(F.get_json_object(F.col("raw_payload"), "$.agency")).alias("agency_code"),
    F.coalesce(
        clean_text(F.get_json_object(F.col("raw_payload"), "$.agency_name")),
        clean_text(F.get_json_object(F.col("raw_payload"), "$.agency")),
    ).alias("agency_name"),
    clean_text(F.get_json_object(F.col("raw_payload"), "$.complaint_type")).alias("complaint_type"),
    clean_text(F.get_json_object(F.col("raw_payload"), "$.descriptor")).alias("descriptor"),
    clean_text(F.get_json_object(F.col("raw_payload"), "$.status")).alias("status_name"),
    clean_text(F.get_json_object(F.col("raw_payload"), "$.borough")).alias("borough_raw"),
    clean_text(F.get_json_object(F.col("raw_payload"), "$.incident_zip")).alias("incident_zip"),
    clean_text(F.get_json_object(F.col("raw_payload"), "$.city")).alias("city"),
    parse_double(F.get_json_object(F.col("raw_payload"), "$.latitude")).alias("latitude"),
    parse_double(F.get_json_object(F.col("raw_payload"), "$.longitude")).alias("longitude"),
    clean_text(F.get_json_object(F.col("raw_payload"), "$.location_type")).alias("location_type"),
    clean_text(F.get_json_object(F.col("raw_payload"), "$.open_data_channel_type")).alias("channel_type"),
    clean_text(F.get_json_object(F.col("raw_payload"), "$.resolution_description")).alias("resolution_description"),
    F.coalesce(F.col("record_hash"), F.sha2(F.col("raw_payload"), 256)).alias("record_hash"),
    F.current_timestamp().alias("load_timestamp"),
)

normalized_borough = F.regexp_replace(F.upper(F.trim(F.col("borough_raw"))), r"\s+", " ")
silver_df = (
    selected_df.withColumn(
        "borough",
        F.when(F.col("borough_raw").isNull(), F.lit(None)).otherwise(
            F.coalesce(borough_map[normalized_borough], normalized_borough)
        ),
    )
    .drop("borough_raw")
    .withColumn("created_year", F.year(F.col("created_at")))
    .withColumn("created_month", F.month(F.col("created_at")))
    .withColumn("created_date", F.to_date(F.col("created_at")))
    .withColumn("closed_date", F.to_date(F.col("closed_at")))
    .withColumn(
        "is_closed",
        F.col("closed_at").isNotNull() | F.lower(F.trim(F.col("status_name"))).isin("closed", "resolved"),
    )
    .withColumn(
        "resolution_time_hours",
        F.when(
            F.col("created_at").isNotNull()
            & F.col("closed_at").isNotNull()
            & (F.col("closed_at") >= F.col("created_at")),
            F.round(
                (F.col("closed_at").cast("long") - F.col("created_at").cast("long")) / F.lit(3600.0),
                2,
            ),
        ),
    )
    .withColumn(
        "is_overdue",
        F.col("due_date").isNotNull()
        & (~F.col("is_closed"))
        & (F.col("due_date") < F.col("load_timestamp")),
    )
)

with_request_id = silver_df.filter(F.col("request_id").isNotNull() & (F.trim(F.col("request_id")) != ""))
without_request_id = silver_df.filter(F.col("request_id").isNull() | (F.trim(F.col("request_id")) == ""))

dedup_window = Window.partitionBy("request_id").orderBy(
    F.col("updated_at").desc_nulls_last(),
    F.col("closed_at").desc_nulls_last(),
    F.col("created_at").desc_nulls_last(),
    F.col("load_timestamp").desc_nulls_last(),
)

deduped_df = (
    with_request_id.withColumn("_row_number", F.row_number().over(dedup_window))
    .filter(F.col("_row_number") == 1)
    .drop("_row_number")
    .unionByName(without_request_id)
)

write_delta_table(
    deduped_df,
    SILVER_TABLE,
    table_path,
    mode="overwrite",
    partition_by=["created_year", "created_month"],
)

print(f"Published {SILVER_TABLE} at {table_path}")
print(f"Silver row count: {deduped_df.count()}")
