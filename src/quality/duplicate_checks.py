"""Starter duplicate-detection helpers."""

from __future__ import annotations

from collections import Counter
from typing import Any, Iterable


def duplicate_key_counts(records: Iterable[dict[str, Any]], key_field: str) -> dict[Any, int]:
    """Return counts for duplicated key values only."""
    counter = Counter(record.get(key_field) for record in records if record.get(key_field) not in (None, ""))
    return {key: count for key, count in counter.items() if count > 1}


def find_duplicate_keys(records: Iterable[dict[str, Any]], key_field: str) -> list[Any]:
    """Return duplicate key values for a chosen field."""
    return list(duplicate_key_counts(records, key_field).keys())


def duplicate_count(records: Iterable[dict[str, Any]], key_field: str) -> int:
    """Return the number of duplicate rows beyond the first occurrence."""
    return sum(count - 1 for count in duplicate_key_counts(records, key_field).values())


def duplicate_ratio(records: Iterable[dict[str, Any]], key_field: str) -> float:
    """Calculate a simple duplicate ratio for quick quality checks."""
    rows = list(records)
    if not rows:
        return 0.0

    return round(duplicate_count(rows, key_field) / len(rows), 4)


def duplicate_summary(records: Iterable[dict[str, Any]], key_field: str) -> dict[str, Any]:
    """Return a compact summary of duplicate-key metrics."""
    rows = list(records)
    return {
        "key_field": key_field,
        "duplicate_key_count": len(find_duplicate_keys(rows, key_field)),
        "duplicate_row_count": duplicate_count(rows, key_field),
        "duplicate_ratio": duplicate_ratio(rows, key_field),
    }
