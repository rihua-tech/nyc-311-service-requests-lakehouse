from src.quality.duplicate_checks import (
    duplicate_count,
    duplicate_key_counts,
    duplicate_ratio,
    duplicate_summary,
    find_duplicate_keys,
)


def test_find_duplicate_keys_returns_repeated_values() -> None:
    rows = [
        {"request_id": "1"},
        {"request_id": "2"},
        {"request_id": "1"},
    ]

    assert find_duplicate_keys(rows, "request_id") == ["1"]


def test_duplicate_ratio_returns_simple_fraction() -> None:
    rows = [
        {"request_id": "1"},
        {"request_id": "2"},
        {"request_id": "1"},
        {"request_id": "3"},
    ]

    assert duplicate_ratio(rows, "request_id") == 0.25


def test_duplicate_helpers_return_counts_and_summary() -> None:
    rows = [
        {"request_id": "1"},
        {"request_id": "1"},
        {"request_id": "1"},
        {"request_id": "2"},
    ]

    assert duplicate_key_counts(rows, "request_id") == {"1": 3}
    assert duplicate_count(rows, "request_id") == 2
    assert duplicate_summary(rows, "request_id") == {
        "key_field": "request_id",
        "duplicate_key_count": 1,
        "duplicate_row_count": 2,
        "duplicate_ratio": 0.5,
    }

