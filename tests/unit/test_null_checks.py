from src.quality.null_checks import count_nulls, has_required_non_nulls


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

