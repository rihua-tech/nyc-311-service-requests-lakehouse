# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Standardize Locations
# MAGIC
# MAGIC Purpose:
# MAGIC - Validate normalized location fields and extract a starter location reference table.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - silver request rows after core cleaning in `01_clean_service_requests`
# MAGIC
# MAGIC Expected outputs:
# MAGIC - reviewed location attributes and starter rows for `silver.location_reference`
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Review borough normalization results from the silver transform module.
# MAGIC 2. Validate ZIP, city, and coordinate coverage.
# MAGIC 3. Build a starter `silver.location_reference` dataset.

# COMMAND ----------
from pyspark.sql import functions as F

from src.common.constants import SILVER_REFERENCE_TABLES, SILVER_TABLE
from src.common.databricks_runtime import bootstrap_notebook, write_delta_table

config = bootstrap_notebook(spark=spark, dbutils=dbutils)  # type: ignore[name-defined]
location_table = SILVER_REFERENCE_TABLES["location"]
table_path = config["paths"]["table_paths"][location_table]

if not spark.catalog.tableExists(SILVER_TABLE):  # type: ignore[name-defined]
    raise ValueError(f"{SILVER_TABLE} does not exist. Run 01_clean_service_requests first.")

silver_df = spark.table(SILVER_TABLE)  # type: ignore[name-defined]
location_df = (
    silver_df.filter(F.col("incident_zip").isNotNull() & (F.trim(F.col("incident_zip")) != ""))
    .groupBy("incident_zip", "borough", "city", "location_type")
    .agg(
        F.first("latitude", ignorenulls=True).alias("latitude"),
        F.first("longitude", ignorenulls=True).alias("longitude"),
    )
    .withColumn("load_timestamp", F.current_timestamp())
)

write_delta_table(location_df, location_table, table_path, mode="overwrite")

print(f"Published {location_table} at {table_path}")
print(f"Location reference row count: {location_df.count()}")
