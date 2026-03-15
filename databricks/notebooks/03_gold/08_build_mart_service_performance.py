# Databricks notebook source
# MAGIC %md
# MAGIC # 08 Build Mart Service Performance
# MAGIC
# MAGIC Purpose:
# MAGIC - Publish a performance mart for closure and resolution analytics.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - `gold.fact_service_requests`
# MAGIC
# MAGIC Expected outputs:
# MAGIC - `gold.mart_service_performance`
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Filter to relevant closed or due requests.
# MAGIC 2. Aggregate by agency and complaint type.
# MAGIC 3. Write the mart.

# COMMAND ----------
print("Scaffold notebook: build gold.mart_service_performance")

# TODO: Align KPI definitions with the metrics-definition document.

