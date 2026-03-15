import json

from src.ingestion.bronze_loader import build_bronze_record
from src.transformation.silver_service_requests import (
    clean_service_request,
    normalize_borough,
    parse_bronze_record,
    remove_duplicate_request_ids,
)


def test_parse_bronze_record_parses_raw_payload_and_metadata() -> None:
    bronze_row = {
        "source_record_id": "1001",
        "record_hash": "abc123",
        "ingest_timestamp": "2024-01-01T10:30:00+00:00",
        "raw_payload": json.dumps({"unique_key": "1001", "borough": "BKLYN"}),
    }

    parsed = parse_bronze_record(bronze_row)

    assert parsed["payload"]["unique_key"] == "1001"
    assert parsed["record_hash"] == "abc123"
    assert parsed["source_record_id"] == "1001"


def test_normalize_borough_maps_common_aliases() -> None:
    assert normalize_borough("BKLYN") == "BROOKLYN"
    assert normalize_borough("new york") == "MANHATTAN"
    assert normalize_borough(None) is None


def test_remove_duplicate_request_ids_keeps_latest_row() -> None:
    rows = [
        {
            "request_id": "1001",
            "created_at": "2024-01-01T10:00:00+00:00",
            "updated_at": "2024-01-01T11:00:00+00:00",
            "closed_at": None,
            "load_timestamp": "2024-01-01T11:05:00+00:00",
            "status_name": "Open",
        },
        {
            "request_id": "1001",
            "created_at": "2024-01-01T10:00:00+00:00",
            "updated_at": "2024-01-01T13:00:00+00:00",
            "closed_at": "2024-01-01T13:00:00+00:00",
            "load_timestamp": "2024-01-01T13:05:00+00:00",
            "status_name": "Closed",
        },
    ]

    deduplicated_rows = remove_duplicate_request_ids(rows)

    assert len(deduplicated_rows) == 1
    assert deduplicated_rows[0]["status_name"] == "Closed"


def test_clean_service_request_derives_overdue_flag_for_open_requests() -> None:
    bronze_row = build_bronze_record(
        {
            "unique_key": "1002",
            "created_date": "2024-01-01T08:00:00",
            "due_date": "2024-01-01T10:00:00",
            "status": "Open",
            "agency": "DEP",
        },
        ingest_timestamp=None,
    )

    row = clean_service_request(bronze_row)

    assert row["is_closed"] is False
    assert row["is_overdue"] is True
