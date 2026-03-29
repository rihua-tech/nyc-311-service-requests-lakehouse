# Databricks notebook source
# MAGIC %md
# MAGIC # 06 Build Fact Service Requests
# MAGIC
# MAGIC Purpose:
# MAGIC - Build the request-level fact table for gold reporting models.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - `silver.service_requests_clean`
# MAGIC - `gold.dim_agency`
# MAGIC - `gold.dim_complaint_type`
# MAGIC - `gold.dim_location`
# MAGIC - `gold.dim_status`
# MAGIC
# MAGIC Expected outputs:
# MAGIC - `gold.fact_service_requests`
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Read silver requests.
# MAGIC 2. Resolve surrogate keys from gold dimensions.
# MAGIC 3. Select fact measures, dates, and degenerate request attributes.
# MAGIC 4. Write the fact table.

# COMMAND ----------
from pyspark.sql import functions as F

from src.common.constants import GOLD_TABLES, SILVER_TABLE
from src.common.databricks_runtime import bootstrap_notebook, write_delta_table

config = bootstrap_notebook(spark=spark, dbutils=dbutils)  # type: ignore[name-defined]
target_table = GOLD_TABLES["fact_service_requests"]
target_path = config["paths"]["table_paths"][target_table]

silver_df = spark.table(SILVER_TABLE).alias("silver")  # type: ignore[name-defined]
agency_df = spark.table(GOLD_TABLES["dim_agency"]).select("agency_code", "agency_sk").alias("agency")  # type: ignore[name-defined]
complaint_df = spark.table(GOLD_TABLES["dim_complaint_type"]).select(  # type: ignore[name-defined]
    "complaint_type",
    "descriptor",
    "complaint_type_sk",
).alias("complaint")
location_df = spark.table(GOLD_TABLES["dim_location"]).select(  # type: ignore[name-defined]
    "incident_zip",
    "borough",
    "city",
    "location_type",
    "location_sk",
).alias("location")
status_df = spark.table(GOLD_TABLES["dim_status"]).select("status_name", "status_sk").alias("status")  # type: ignore[name-defined]

fact_df = (
    silver_df.join(agency_df, on="agency_code", how="left")
    .join(complaint_df, on=["complaint_type", "descriptor"], how="left")
    .join(location_df, on=["incident_zip", "borough", "city", "location_type"], how="left")
    .join(status_df, on="status_name", how="left")
    .select(
        F.col("request_id"),
        F.when(F.col("created_date").isNull(), F.lit(None)).otherwise(
            F.date_format(F.col("created_date"), "yyyyMMdd").cast("int")
        ).alias("created_date_key"),
        F.when(F.col("closed_date").isNull(), F.lit(None)).otherwise(
            F.date_format(F.col("closed_date"), "yyyyMMdd").cast("int")
        ).alias("closed_date_key"),
        F.coalesce(F.col("agency_sk"), F.lit(0)).cast("int").alias("agency_sk"),
        F.coalesce(F.col("complaint_type_sk"), F.lit(0)).cast("int").alias("complaint_type_sk"),
        F.coalesce(F.col("location_sk"), F.lit(0)).cast("int").alias("location_sk"),
        F.coalesce(F.col("status_sk"), F.lit(0)).cast("int").alias("status_sk"),
        F.col("created_at"),
        F.col("closed_at"),
        F.col("due_date"),
        F.col("created_date"),
        F.col("closed_date"),
        F.col("agency_code"),
        F.col("complaint_type"),
        F.col("descriptor"),
        F.col("status_name"),
        F.col("incident_zip"),
        F.col("borough"),
        F.col("city"),
        F.col("location_type"),
        F.col("channel_type"),
        F.col("resolution_time_hours"),
        F.col("is_closed"),
        F.col("is_overdue"),
        F.col("record_hash"),
        F.col("load_timestamp"),
    )
)

write_delta_table(fact_df, target_table, target_path, mode="overwrite")

print(f"Published {target_table} at {target_path}")
print(f"Fact row count: {fact_df.count()}")
