# Databricks notebook source
# MAGIC %md
# MAGIC # 06 Build Fact Service Requests
# MAGIC
# MAGIC Purpose:
# MAGIC - Build the request-level fact table for gold reporting models.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - `silver.service_requests_clean`
# MAGIC - gold dimensions for lookups
# MAGIC
# MAGIC Expected outputs:
# MAGIC - `gold.fact_service_requests`
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Read silver requests.
# MAGIC 2. Join dimension keys.
# MAGIC 3. Select fact measures and flags.
# MAGIC 4. Write the fact table.

# COMMAND ----------
print("Scaffold notebook: build gold.fact_service_requests")

# TODO: Finalize fact grain and foreign key lookups.

