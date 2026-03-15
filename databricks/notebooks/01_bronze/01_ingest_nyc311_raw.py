# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Ingest NYC311 Raw
# MAGIC
# MAGIC Purpose:
# MAGIC - Load raw NYC 311 API data into the bronze layer.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - `environment`
# MAGIC - raw landing path or API extraction output
# MAGIC - optional watermark value
# MAGIC
# MAGIC Expected outputs:
# MAGIC - writes to `bronze.nyc311_service_requests_raw`
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Load config.
# MAGIC 2. Read landed raw data.
# MAGIC 3. Attach ingest metadata.
# MAGIC 4. Write Delta bronze rows.

# COMMAND ----------
print("Scaffold notebook: ingest NYC 311 raw data")

# TODO: Import `src.ingestion.api_extract` and `src.ingestion.bronze_loader`.

