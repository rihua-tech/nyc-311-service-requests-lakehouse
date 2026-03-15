from datetime import datetime, timezone

from src.ingestion.bronze_loader import build_bronze_file_path, prepare_bronze_batch


def test_build_bronze_file_path_uses_partition_style_layout() -> None:
    ingest_timestamp = datetime(2024, 1, 2, 9, 30, tzinfo=timezone.utc)

    path = build_bronze_file_path(
        bronze_base_path="abfss://raw@storage.dfs.core.windows.net/nyc311/service_requests/raw/",
        ingest_timestamp=ingest_timestamp,
        batch_id="batch-001",
    )

    assert path.endswith("/ingest_date=2024-01-02/batch_batch-001.json")


def test_prepare_bronze_batch_applies_shared_batch_metadata() -> None:
    ingest_timestamp = datetime(2024, 1, 2, 9, 30, tzinfo=timezone.utc)

    rows = prepare_bronze_batch(
        records=[
            {"unique_key": "1001", "complaint_type": "Noise"},
            {"unique_key": "1002", "complaint_type": "Water Leak"},
        ],
        bronze_base_path="abfss://raw@storage.dfs.core.windows.net/nyc311/service_requests/raw/",
        api_pull_timestamp="2024-01-02T09:00:00+00:00",
        ingest_timestamp=ingest_timestamp,
    )

    assert len(rows) == 2
    assert rows[0]["ingest_date"] == "2024-01-02"
    assert rows[0]["api_pull_timestamp"] == "2024-01-02T09:00:00+00:00"
    assert rows[0]["file_path"] == rows[1]["file_path"]
    assert rows[0]["source_record_id"] == "1001"
    assert rows[1]["source_record_id"] == "1002"
