# Databricks notebook source
# MAGIC %md
# MAGIC # 00 Mounts And Paths
# MAGIC
# MAGIC Purpose:
# MAGIC - Document how storage mounts and workspace paths may be configured.
# MAGIC
# MAGIC Expected inputs:
# MAGIC - `environment`
# MAGIC - storage account and container names from config
# MAGIC
# MAGIC Expected outputs:
# MAGIC - validated base paths for raw, bronze, silver, and gold data
# MAGIC
# MAGIC Major steps:
# MAGIC 1. Read environment widgets.
# MAGIC 2. Resolve storage account and container paths.
# MAGIC 3. Optionally verify mount points or direct ABFSS access.
# MAGIC 4. Persist shared path variables for downstream notebooks.

# COMMAND ----------
from datetime import datetime, timezone

from src.common.databricks_runtime import bootstrap_notebook

try:
    config = bootstrap_notebook(
        spark=spark,  # type: ignore[name-defined]
        dbutils=dbutils,  # type: ignore[name-defined]
        extra_widget_defaults={
            "page_size": "",
            "max_pages_per_run": "",
        },
    )
    environment = config["environment"]
    paths = config["paths"]

    smoke_test_path = f"{paths['checkpoint_base_path'].rstrip('/')}/_setup_storage_smoke_test"
    smoke_test_rows = [
        {
            "validated_at": datetime.now(timezone.utc).isoformat(),
            "environment": environment,
            "storage_account": config["azure"]["storage_account"],
            "filesystem": config["azure"]["container"],
        }
    ]

    spark.createDataFrame(smoke_test_rows).write.mode("overwrite").json(smoke_test_path)  # type: ignore[name-defined]
    smoke_test_count = spark.read.json(smoke_test_path).count()  # type: ignore[name-defined]

    print(f"Environment: {environment}")
    print(f"Storage account: {config['azure']['storage_account']}")
    print(f"Filesystem: {config['azure']['container']}")
    print(f"Active catalog: {config['databricks']['catalog']}")
    print(f"Bronze base path: {paths['bronze_base_path']}")
    print(f"Silver base path: {paths['silver_base_path']}")
    print(f"Gold base path: {paths['gold_base_path']}")
    print(f"Checkpoint base path: {paths['checkpoint_base_path']}")
    print(f"Watermark path: {config['source']['watermark_state_path']}")
    print(f"ADLS smoke-test row count: {smoke_test_count}")
except NameError:
    print("This notebook is intended to run inside Databricks.")
