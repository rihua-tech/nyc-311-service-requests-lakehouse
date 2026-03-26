# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Build Dim Date
# MAGIC
# MAGIC Purpose:
# MAGIC - Build the gold date dimension used by downstream facts and marts.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - distinct `created_date` and `closed_date` values from `silver.service_requests_clean`
# MAGIC
# MAGIC Expected outputs:
# MAGIC - `gold.dim_date`
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Collect distinct date values from the silver request dataset.
# MAGIC 2. Generate calendar attributes such as quarter, weekday, and weekend flags.
# MAGIC 3. Write the date dimension to Delta.

# COMMAND ----------
from pyspark.sql import functions as F

from src.common.constants import GOLD_TABLES, SILVER_TABLE
from src.common.databricks_runtime import bootstrap_notebook, write_delta_table

config = bootstrap_notebook(spark=spark, dbutils=dbutils)  # type: ignore[name-defined]
table_name = GOLD_TABLES["dim_date"]
table_path = config["paths"]["table_paths"][table_name]

if not spark.catalog.tableExists(SILVER_TABLE):  # type: ignore[name-defined]
    raise ValueError(f"{SILVER_TABLE} does not exist. Run the silver notebooks first.")

silver_df = spark.table(SILVER_TABLE)  # type: ignore[name-defined]
date_df = (
    silver_df.select(F.col("created_date").alias("calendar_date"))
    .unionByName(silver_df.select(F.col("closed_date").alias("calendar_date")))
    .filter(F.col("calendar_date").isNotNull())
    .distinct()
    .withColumn("date_key", F.date_format(F.col("calendar_date"), "yyyyMMdd").cast("int"))
    .withColumn("calendar_year", F.year(F.col("calendar_date")))
    .withColumn("calendar_quarter", F.quarter(F.col("calendar_date")))
    .withColumn("calendar_month", F.month(F.col("calendar_date")))
    .withColumn("month_name", F.date_format(F.col("calendar_date"), "MMMM"))
    .withColumn("calendar_day", F.dayofmonth(F.col("calendar_date")))
    .withColumn("day_of_week", (((F.dayofweek(F.col("calendar_date")) + F.lit(5)) % F.lit(7)) + F.lit(1)).cast("int"))
    .withColumn("day_name", F.date_format(F.col("calendar_date"), "EEEE"))
    .withColumn("is_weekend", F.col("day_of_week") >= F.lit(6))
    .select(
        "date_key",
        "calendar_date",
        "calendar_year",
        "calendar_quarter",
        "calendar_month",
        "month_name",
        "calendar_day",
        "day_of_week",
        "day_name",
        "is_weekend",
    )
)

write_delta_table(date_df, table_name, table_path, mode="overwrite")

print(f"Published {table_name} at {table_path}")
print(f"dim_date row count: {date_df.count()}")
