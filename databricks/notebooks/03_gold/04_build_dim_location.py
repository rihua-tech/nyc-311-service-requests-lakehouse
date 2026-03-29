# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Build Dim Location
# MAGIC
# MAGIC Purpose:
# MAGIC - Build a starter location dimension for borough and ZIP-based analysis.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - `silver.location_reference`
# MAGIC
# MAGIC Expected outputs:
# MAGIC - `gold.dim_location`
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Read location reference rows.
# MAGIC 2. Standardize location business keys and assign surrogate keys.
# MAGIC 3. Write the dimension.

# COMMAND ----------
from pyspark.sql import Window
from pyspark.sql import functions as F

from src.common.constants import GOLD_TABLES, SILVER_REFERENCE_TABLES
from src.common.databricks_runtime import bootstrap_notebook, write_delta_table

config = bootstrap_notebook(spark=spark, dbutils=dbutils)  # type: ignore[name-defined]
source_table = SILVER_REFERENCE_TABLES["location"]
target_table = GOLD_TABLES["dim_location"]

location_df = (
    spark.table(source_table)  # type: ignore[name-defined]
    .orderBy(
        F.col("incident_zip").asc_nulls_last(),
        F.col("borough").asc_nulls_last(),
        F.col("city").asc_nulls_last(),
        F.col("location_type").asc_nulls_last(),
    )
    .withColumn(
        "location_sk",
        F.row_number().over(
            Window.orderBy(
                F.col("incident_zip").asc_nulls_last(),
                F.col("borough").asc_nulls_last(),
                F.col("city").asc_nulls_last(),
                F.col("location_type").asc_nulls_last(),
            )
        ),
    )
    .withColumn("effective_timestamp", F.current_timestamp())
    .select(
        "location_sk",
        "incident_zip",
        "borough",
        "city",
        "location_type",
        "latitude",
        "longitude",
        "effective_timestamp",
    )
)

write_delta_table(location_df, target_table, config["paths"]["table_paths"][target_table], mode="overwrite")

print(f"Published {target_table}")
print(f"dim_location row count: {location_df.count()}")
