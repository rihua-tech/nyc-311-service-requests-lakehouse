from src.quality.duplicate_checks import duplicate_ratio, find_duplicate_keys


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

