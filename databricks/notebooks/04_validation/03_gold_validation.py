# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Gold Validation
# MAGIC
# MAGIC Purpose:
# MAGIC - Run starter validation checks for gold dimensions, facts, and marts.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - gold dimensions, fact tables, and reporting marts
# MAGIC
# MAGIC Expected outputs:
# MAGIC - scaffolded uniqueness, reconciliation, and aggregate sanity checks
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Validate dimension uniqueness and null-safe business keys.
# MAGIC 2. Reconcile fact row counts with `silver.service_requests_clean`.
# MAGIC 3. Spot-check mart aggregates once gold logic is implemented.

# COMMAND ----------
from src.quality.rowcount_checks import layer_reconciliation_summaries

sample_layer_counts = {
    "silver": 0,
    "gold_fact": 0,
    "gold_request_volume_mart": 0,
}

print(
    "Scaffold notebook: validate gold layer "
    f"with reconciliation summaries={layer_reconciliation_summaries(sample_layer_counts, baseline_layer='silver')}"
)

# TODO: Add business-rule checks once KPI definitions are finalized.
