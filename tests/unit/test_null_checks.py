from src.quality.null_checks import (
    count_nulls,
    has_required_non_nulls,
    is_nullish,
    null_counts_by_field,
    required_field_failures,
)


def test_is_nullish_treats_none_and_blank_as_empty() -> None:
    assert is_nullish(None) is True
    assert is_nullish("") is True
    assert is_nullish("value") is False


def test_count_nulls_counts_none_and_blank_values() -> None:
    rows = [
        {"request_id": "1"},
        {"request_id": None},
        {"request_id": ""},
    ]

    assert count_nulls(rows, "request_id") == 2


def test_has_required_non_nulls_flags_missing_values() -> None:
    rows = [
        {"request_id": "1", "agency_code": "NYPD"},
        {"request_id": "2", "agency_code": ""},
    ]

    assert has_required_non_nulls(rows, ["request_id", "agency_code"]) is False


def test_null_counts_by_field_and_required_field_failures_return_summaries() -> None:
    rows = [
        {"request_id": "1", "created_at": "2024-01-01T10:00:00"},
        {"request_id": "", "created_at": None},
    ]

    assert null_counts_by_field(rows, ["request_id", "created_at"]) == {
        "request_id": 1,
        "created_at": 1,
    }
    assert required_field_failures(rows, ["request_id", "created_at"]) == {
        "request_id": 1,
        "created_at": 1,
    }
