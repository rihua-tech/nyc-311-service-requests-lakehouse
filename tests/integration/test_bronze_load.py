from src.ingestion.bronze_loader import build_bronze_record, prepare_bronze_batch


def test_build_bronze_record_adds_expected_metadata() -> None:
    record = build_bronze_record({"unique_key": "1001", "complaint_type": "Noise"})

    assert record["source_record_id"] == "1001"
    assert record["source_system"] == "nyc_311_api"
    assert "record_hash" in record


def test_prepare_bronze_batch_returns_one_row_per_input() -> None:
    rows = prepare_bronze_batch(
        [
            {"unique_key": "1001"},
            {"unique_key": "1002"},
        ]
    )

    assert len(rows) == 2

