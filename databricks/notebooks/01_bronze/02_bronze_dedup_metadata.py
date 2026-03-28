# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Bronze Dedup Metadata
# MAGIC
# MAGIC Purpose:
# MAGIC - Add lightweight deduplication and lineage handling in bronze.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - raw bronze staging rows
# MAGIC
# MAGIC Expected outputs:
# MAGIC - deduplicated bronze dataset with metadata fields confirmed
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Inspect duplicate hashes or source ids.
# MAGIC 2. Apply starter deduplication rules.
# MAGIC 3. Persist curated bronze output.

# COMMAND ----------
import importlib

from pyspark.sql import Window
from pyspark.sql import functions as F

from src.common.constants import BRONZE_TABLE
from src.common import databricks_runtime as runtime

runtime = importlib.reload(runtime)

config = runtime.bootstrap_notebook(  # type: ignore[name-defined]
    spark=spark,
    dbutils=dbutils,
    configure_storage_access=False,
)
table_path = config["paths"]["table_paths"][BRONZE_TABLE]

if not spark.catalog.tableExists(BRONZE_TABLE):  # type: ignore[name-defined]
    raise ValueError(f"{BRONZE_TABLE} does not exist. Run 01_ingest_nyc311_raw first.")

bronze_df = spark.table(BRONZE_TABLE)  # type: ignore[name-defined]
input_count = bronze_df.count()

window_spec = Window.partitionBy("_dedup_key").orderBy(
    F.col("ingest_timestamp").desc_nulls_last(),
    F.col("api_pull_timestamp").desc_nulls_last(),
    F.col("ingest_id").desc_nulls_last(),
)

deduped_df = (
    bronze_df.withColumn("_dedup_key", F.coalesce(F.col("record_hash"), F.col("ingest_id")))
    .withColumn("_row_number", F.row_number().over(window_spec))
    .filter(F.col("_row_number") == 1)
    .drop("_dedup_key", "_row_number")
)

output_count = deduped_df.count()
runtime.write_delta_table(deduped_df, BRONZE_TABLE, table_path, mode="overwrite")

print(f"Bronze input row count: {input_count}")
print(f"Bronze output row count: {output_count}")
print(f"Removed duplicate rows: {input_count - output_count}")
print(f"Published bronze table at: {table_path}")
