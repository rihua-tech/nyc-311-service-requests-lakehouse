# Databricks notebook source
# MAGIC %md
# MAGIC # 09 Build Mart Backlog Snapshot
# MAGIC
# MAGIC Purpose:
# MAGIC - Publish a backlog snapshot mart for open request monitoring.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - `gold.fact_service_requests`
# MAGIC
# MAGIC Expected outputs:
# MAGIC - `gold.mart_backlog_snapshot`
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Choose a snapshot date for backlog measurement.
# MAGIC 2. Filter request facts that are still open as of that date.
# MAGIC 3. Group by `status_name` and `agency_code`.
# MAGIC 4. Write the snapshot mart.

# COMMAND ----------
from pyspark.sql import functions as F

from src.common.constants import GOLD_TABLES
from src.common.databricks_runtime import bootstrap_notebook, get_widget, write_delta_table

config = bootstrap_notebook(
    spark=spark,  # type: ignore[name-defined]
    dbutils=dbutils,  # type: ignore[name-defined]
    extra_widget_defaults={"snapshot_date": ""},
)
target_table = GOLD_TABLES["mart_backlog_snapshot"]
snapshot_date = get_widget(dbutils, "snapshot_date", default="")  # type: ignore[name-defined]
run_date = str(config.get("runtime", {}).get("run_date") or "").strip()
effective_as_of_date = snapshot_date or run_date

fact_df = spark.table(GOLD_TABLES["fact_service_requests"])  # type: ignore[name-defined]
snapshot_expr = F.to_date(F.lit(effective_as_of_date)) if effective_as_of_date else F.current_date()
open_df = fact_df.filter(
    (F.col("created_date") <= snapshot_expr)
    & (
        (~F.col("is_closed"))
        | (F.col("is_closed") & F.col("closed_date").isNotNull() & (F.col("closed_date") > snapshot_expr))
    )
)
mart_df = (
    open_df.groupBy("status_name", "agency_code")
    .agg(F.count("*").alias("open_request_count"))
    .withColumn("snapshot_date", snapshot_expr)
    .select("snapshot_date", "status_name", "agency_code", "open_request_count")
)

write_delta_table(
    mart_df,
    target_table,
    config["paths"]["table_paths"][target_table],
    mode="overwrite",
)

print(f"Published {target_table}")
print(f"Snapshot date: {effective_as_of_date or '<current_date>'}")
print(f"mart_backlog_snapshot row count: {mart_df.count()}")
