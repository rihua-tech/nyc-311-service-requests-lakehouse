"""Starter duplicate-detection helpers."""

from __future__ import annotations

from collections import Counter
from typing import Any, Iterable


def find_duplicate_keys(records: Iterable[dict[str, Any]], key_field: str) -> list[Any]:
    """Return duplicate key values for a chosen field."""
    counter = Counter(record.get(key_field) for record in records if record.get(key_field) not in (None, ""))
    return [key for key, count in counter.items() if count > 1]


def duplicate_ratio(records: Iterable[dict[str, Any]], key_field: str) -> float:
    """Calculate a simple duplicate ratio for quick quality checks."""
    rows = list(records)
    if not rows:
        return 0.0

    duplicates = len(find_duplicate_keys(rows, key_field))
    return round(duplicates / len(rows), 4)

