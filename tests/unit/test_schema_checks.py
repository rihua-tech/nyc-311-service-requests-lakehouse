from src.quality.schema_checks import (
    has_expected_types,
    missing_fields,
    schema_matches,
    type_mismatches,
    unexpected_fields,
)


def test_missing_and_unexpected_fields_are_reported() -> None:
    record = {"request_id": "1001", "created_at": "2024-01-01T10:00:00", "extra_field": "value"}
    expected_fields = ["request_id", "created_at", "borough"]

    assert missing_fields(record, expected_fields) == ["borough"]
    assert unexpected_fields(record, expected_fields) == ["extra_field"]
    assert schema_matches(record, expected_fields) is False


def test_type_mismatches_and_has_expected_types_cover_simple_runtime_checks() -> None:
    record = {"request_id": 1001, "resolution_time_hours": "2.5", "is_closed": True}
    expected_types = {
        "request_id": str,
        "resolution_time_hours": (float, int),
        "is_closed": bool,
    }

    assert type_mismatches(record, expected_types) == {
        "request_id": "str",
        "resolution_time_hours": "float, int",
    }
    assert has_expected_types(record, expected_types) is False

