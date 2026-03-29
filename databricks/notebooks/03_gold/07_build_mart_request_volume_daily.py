# Databricks notebook source
# MAGIC %md
# MAGIC # 07 Build Mart Request Volume Daily
# MAGIC
# MAGIC Purpose:
# MAGIC - Publish a daily request volume mart for trend reporting.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - `gold.fact_service_requests`
# MAGIC
# MAGIC Expected outputs:
# MAGIC - `gold.mart_request_volume_daily`
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Read request-level fact rows.
# MAGIC 2. Aggregate request counts by `created_date`.
# MAGIC 3. Write the daily volume mart.

# COMMAND ----------
from pyspark.sql import functions as F

from src.common.constants import GOLD_TABLES
from src.common.databricks_runtime import bootstrap_notebook, write_delta_table

config = bootstrap_notebook(spark=spark, dbutils=dbutils)  # type: ignore[name-defined]
target_table = GOLD_TABLES["mart_request_volume_daily"]

fact_df = spark.table(GOLD_TABLES["fact_service_requests"])  # type: ignore[name-defined]
mart_df = fact_df.groupBy("created_date").agg(F.count("*").alias("request_count"))

write_delta_table(
    mart_df,
    target_table,
    config["paths"]["table_paths"][target_table],
    mode="overwrite",
)

print(f"Published {target_table}")
print(f"mart_request_volume_daily row count: {mart_df.count()}")
