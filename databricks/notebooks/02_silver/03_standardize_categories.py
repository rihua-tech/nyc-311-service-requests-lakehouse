# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Standardize Categories
# MAGIC
# MAGIC Purpose:
# MAGIC - Extract starter agency, complaint type, and status reference tables from cleaned silver rows.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - cleaned silver request rows after core field selection and deduplication
# MAGIC
# MAGIC Expected outputs:
# MAGIC - starter rows for `silver.agency_reference`, `silver.complaint_type_reference`, and `silver.status_reference`
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Review agency, complaint, and status fields from the silver table.
# MAGIC 2. Apply light normalization where values are obviously blank or inconsistent.
# MAGIC 3. Materialize reference-table starter datasets.

# COMMAND ----------
from pyspark.sql import functions as F

from src.common.constants import SILVER_REFERENCE_TABLES, SILVER_TABLE
from src.common.databricks_runtime import bootstrap_notebook, write_delta_table

config = bootstrap_notebook(spark=spark, dbutils=dbutils)  # type: ignore[name-defined]

if not spark.catalog.tableExists(SILVER_TABLE):  # type: ignore[name-defined]
    raise ValueError(f"{SILVER_TABLE} does not exist. Run 01_clean_service_requests first.")

silver_df = spark.table(SILVER_TABLE)  # type: ignore[name-defined]

agency_table = SILVER_REFERENCE_TABLES["agency"]
agency_df = (
    silver_df.filter(F.col("agency_code").isNotNull() & (F.trim(F.col("agency_code")) != ""))
    .groupBy("agency_code", "agency_name")
    .agg(F.min("created_date").alias("first_seen_created_date"))
    .withColumn("load_timestamp", F.current_timestamp())
)
write_delta_table(agency_df, agency_table, config["paths"]["table_paths"][agency_table], mode="overwrite")

complaint_table = SILVER_REFERENCE_TABLES["complaint_type"]
complaint_df = (
    silver_df.filter(F.col("complaint_type").isNotNull() & (F.trim(F.col("complaint_type")) != ""))
    .groupBy("complaint_type", "descriptor")
    .agg(F.min("created_date").alias("first_seen_created_date"))
    .withColumn("load_timestamp", F.current_timestamp())
)
write_delta_table(
    complaint_df,
    complaint_table,
    config["paths"]["table_paths"][complaint_table],
    mode="overwrite",
)

status_table = SILVER_REFERENCE_TABLES["status"]
status_df = (
    silver_df.filter(F.col("status_name").isNotNull() & (F.trim(F.col("status_name")) != ""))
    .select("status_name")
    .distinct()
    .withColumn(
        "is_closed_status",
        F.lower(F.trim(F.col("status_name"))).isin("closed", "resolved"),
    )
    .withColumn(
        "status_group",
        F.when(F.col("is_closed_status"), F.lit("Closed")).otherwise(F.lit("Open")),
    )
    .withColumn("load_timestamp", F.current_timestamp())
)
write_delta_table(status_df, status_table, config["paths"]["table_paths"][status_table], mode="overwrite")

print(
    "Published silver reference tables "
    f"(agency={agency_df.count()}, complaint_type={complaint_df.count()}, status={status_df.count()})"
)
