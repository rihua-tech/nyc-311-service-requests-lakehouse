# Databricks notebook source
# MAGIC %md
# MAGIC # 05 Build Dim Status
# MAGIC
# MAGIC Purpose:
# MAGIC - Build the request status dimension.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - `silver.status_reference`
# MAGIC
# MAGIC Expected outputs:
# MAGIC - `gold.dim_status`
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Read status reference data.
# MAGIC 2. Assign surrogate keys and retain starter status grouping fields.
# MAGIC 3. Persist the dimension.

# COMMAND ----------
from pyspark.sql import Window
from pyspark.sql import functions as F

from src.common.constants import GOLD_TABLES, SILVER_REFERENCE_TABLES
from src.common.databricks_runtime import bootstrap_notebook, write_delta_table

config = bootstrap_notebook(spark=spark, dbutils=dbutils)  # type: ignore[name-defined]
source_table = SILVER_REFERENCE_TABLES["status"]
target_table = GOLD_TABLES["dim_status"]

status_df = (
    spark.table(source_table)  # type: ignore[name-defined]
    .orderBy(F.col("status_name").asc_nulls_last())
    .withColumn("status_sk", F.row_number().over(Window.orderBy(F.col("status_name").asc_nulls_last())))
    .withColumn("effective_timestamp", F.current_timestamp())
    .select("status_sk", "status_name", "status_group", "is_closed_status", "effective_timestamp")
)

write_delta_table(status_df, target_table, config["paths"]["table_paths"][target_table], mode="overwrite")

print(f"Published {target_table}")
print(f"dim_status row count: {status_df.count()}")
