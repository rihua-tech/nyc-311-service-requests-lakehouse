# Databricks notebook source
# MAGIC %md
# MAGIC # 08 Build Mart Service Performance
# MAGIC
# MAGIC Purpose:
# MAGIC - Publish a performance mart for closure and resolution analytics.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - `gold.fact_service_requests`
# MAGIC
# MAGIC Expected outputs:
# MAGIC - `gold.mart_service_performance`
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Filter to closed request facts.
# MAGIC 2. Aggregate by `agency_code` and `complaint_type`.
# MAGIC 3. Calculate closed request counts and average resolution time.
# MAGIC 4. Write the mart.

# COMMAND ----------
from pyspark.sql import functions as F

from src.common.constants import GOLD_TABLES
from src.common.databricks_runtime import bootstrap_notebook, write_delta_table

config = bootstrap_notebook(spark=spark, dbutils=dbutils)  # type: ignore[name-defined]
target_table = GOLD_TABLES["mart_service_performance"]

fact_df = spark.table(GOLD_TABLES["fact_service_requests"])  # type: ignore[name-defined]
mart_df = (
    fact_df.filter(F.col("is_closed"))
    .groupBy("agency_code", "complaint_type")
    .agg(
        F.count("*").alias("closed_requests"),
        F.round(F.avg("resolution_time_hours"), 2).alias("avg_resolution_time_hours"),
    )
)

write_delta_table(
    mart_df,
    target_table,
    config["paths"]["table_paths"][target_table],
    mode="overwrite",
)

print(f"Published {target_table}")
print(f"mart_service_performance row count: {mart_df.count()}")
