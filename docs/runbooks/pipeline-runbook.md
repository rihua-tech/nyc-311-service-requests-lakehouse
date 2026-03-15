# Pipeline Runbook

## Purpose

Starter operational notes for running the pipeline once implementation work begins.

## Planned Sequence

1. Trigger or schedule the ADF ingestion pipeline.
2. Confirm raw files land in the expected ADLS path.
3. Run Databricks bronze ingestion notebook.
4. Run silver standardization notebooks.
5. Run gold build notebooks.
6. Execute validation notebooks and SQL checks.

## Pre-Run Checklist

- configuration placeholders replaced for the target environment
- Databricks secrets available
- storage paths confirmed
- API rate limits and pagination settings reviewed

## TODO

- add exact job names and workflow dependencies
- add rollback guidance for failed silver or gold runs
- add alerting and ownership details

