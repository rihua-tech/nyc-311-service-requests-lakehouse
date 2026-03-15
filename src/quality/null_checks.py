"""Starter null-check utilities for curated datasets."""

from __future__ import annotations

from typing import Any, Iterable


def count_nulls(records: Iterable[dict[str, Any]], field: str) -> int:
    """Count rows where a field is null or blank."""
    return sum(1 for record in records if record.get(field) in (None, ""))


def has_required_non_nulls(records: Iterable[dict[str, Any]], fields: list[str]) -> bool:
    """Return `True` when all required fields are present in every record."""
    for record in records:
        for field in fields:
            if record.get(field) in (None, ""):
                return False
    return True

