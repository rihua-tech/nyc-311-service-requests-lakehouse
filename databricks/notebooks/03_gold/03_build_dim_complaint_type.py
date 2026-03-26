# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Build Dim Complaint Type
# MAGIC
# MAGIC Purpose:
# MAGIC - Build the complaint type dimension for reporting slices.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - `silver.complaint_type_reference`
# MAGIC
# MAGIC Expected outputs:
# MAGIC - `gold.dim_complaint_type`
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Read complaint reference rows.
# MAGIC 2. Deduplicate complaint type and descriptor pairs and assign surrogate keys.
# MAGIC 3. Write the dimension.

# COMMAND ----------
from pyspark.sql import Window
from pyspark.sql import functions as F

from src.common.constants import GOLD_TABLES, SILVER_REFERENCE_TABLES
from src.common.databricks_runtime import bootstrap_notebook, write_delta_table

config = bootstrap_notebook(spark=spark, dbutils=dbutils)  # type: ignore[name-defined]
source_table = SILVER_REFERENCE_TABLES["complaint_type"]
target_table = GOLD_TABLES["dim_complaint_type"]

complaint_df = (
    spark.table(source_table)  # type: ignore[name-defined]
    .orderBy(F.col("complaint_type").asc_nulls_last(), F.col("descriptor").asc_nulls_last())
    .withColumn(
        "complaint_type_sk",
        F.row_number().over(
            Window.orderBy(F.col("complaint_type").asc_nulls_last(), F.col("descriptor").asc_nulls_last())
        ),
    )
    .withColumn("effective_timestamp", F.current_timestamp())
    .select(
        "complaint_type_sk",
        "complaint_type",
        "descriptor",
        "first_seen_created_date",
        "effective_timestamp",
    )
)

write_delta_table(
    complaint_df,
    target_table,
    config["paths"]["table_paths"][target_table],
    mode="overwrite",
)

print(f"Published {target_table}")
print(f"dim_complaint_type row count: {complaint_df.count()}")
