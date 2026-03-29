# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Build Dim Agency
# MAGIC
# MAGIC Purpose:
# MAGIC - Build the agency dimension from `silver.agency_reference`.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - `silver.agency_reference`
# MAGIC
# MAGIC Expected outputs:
# MAGIC - `gold.dim_agency`
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Read agency reference rows.
# MAGIC 2. Assign stable starter surrogate keys from business keys.
# MAGIC 3. Persist the dimension.

# COMMAND ----------
from pyspark.sql import Window
from pyspark.sql import functions as F

from src.common.constants import GOLD_TABLES, SILVER_REFERENCE_TABLES
from src.common.databricks_runtime import bootstrap_notebook, write_delta_table

config = bootstrap_notebook(spark=spark, dbutils=dbutils)  # type: ignore[name-defined]
source_table = SILVER_REFERENCE_TABLES["agency"]
target_table = GOLD_TABLES["dim_agency"]

agency_df = (
    spark.table(source_table)  # type: ignore[name-defined]
    .orderBy(F.col("agency_code").asc_nulls_last())
    .withColumn("agency_sk", F.row_number().over(Window.orderBy(F.col("agency_code").asc_nulls_last())))
    .withColumn("effective_timestamp", F.current_timestamp())
    .select("agency_sk", "agency_code", "agency_name", "first_seen_created_date", "effective_timestamp")
)

write_delta_table(agency_df, target_table, config["paths"]["table_paths"][target_table], mode="overwrite")

print(f"Published {target_table}")
print(f"dim_agency row count: {agency_df.count()}")
