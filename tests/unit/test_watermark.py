from pathlib import Path

from src.ingestion.watermark import (
    build_watermark_state,
    get_latest_watermark,
    read_watermark_state,
    write_watermark_state,
)


def test_read_watermark_state_returns_empty_shape_for_missing_file(tmp_path: Path) -> None:
    state = read_watermark_state(tmp_path / "missing.json")

    assert state == {"column_name": None, "value": None, "updated_at": None}


def test_write_watermark_state_persists_state(tmp_path: Path) -> None:
    state_path = tmp_path / "watermark.json"

    written_state = write_watermark_state(
        path=state_path,
        column_name="created_date",
        value="2024-01-02T09:30:00",
    )
    read_state = read_watermark_state(state_path)

    assert written_state["column_name"] == "created_date"
    assert written_state["value"] == "2024-01-02T09:30:00"
    assert read_state["column_name"] == "created_date"
    assert read_state["value"] == "2024-01-02T09:30:00"
    assert read_state["updated_at"] is not None


def test_build_watermark_state_and_get_latest_watermark_are_simple() -> None:
    state = build_watermark_state("created_date", "2024-01-03T00:00:00")

    assert state["column_name"] == "created_date"
    assert get_latest_watermark(["2024-01-01", None, "2024-01-05"]) == "2024-01-05"

