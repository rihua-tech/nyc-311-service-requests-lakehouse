# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Secrets And Widgets
# MAGIC
# MAGIC Purpose:
# MAGIC - Define notebook widgets and outline secret retrieval requirements.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - `environment`
# MAGIC - `run_date`
# MAGIC
# MAGIC Expected outputs:
# MAGIC - notebook parameters available to downstream tasks
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Create widgets.
# MAGIC 2. Read secrets from a secret scope.
# MAGIC 3. Validate required runtime values.

# COMMAND ----------
try:
    environment = dbutils.widgets.get("environment")  # type: ignore[name-defined]
except NameError:
    environment = "dev"

print(f"Scaffold notebook for widgets and secrets: {environment}")

# TODO: Add real `dbutils.widgets` and `dbutils.secrets.get` calls in Databricks.

