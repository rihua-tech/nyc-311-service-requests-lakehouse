"""Starter null-check utilities for curated datasets."""

from __future__ import annotations

from typing import Any, Iterable


def is_nullish(value: Any) -> bool:
    """Return `True` for values treated as empty in starter checks."""
    return value in (None, "")


def count_nulls(records: Iterable[dict[str, Any]], field: str) -> int:
    """Count rows where a field is null or blank."""
    return sum(1 for record in records if is_nullish(record.get(field)))


def null_counts_by_field(records: Iterable[dict[str, Any]], fields: list[str]) -> dict[str, int]:
    """Return null counts for a list of fields."""
    rows = list(records)
    return {field: count_nulls(rows, field) for field in fields}


def required_field_failures(records: Iterable[dict[str, Any]], fields: list[str]) -> dict[str, int]:
    """Return a summary of required-field failures."""
    rows = list(records)
    return {field: count_nulls(rows, field) for field in fields}


def has_required_non_nulls(records: Iterable[dict[str, Any]], fields: list[str]) -> bool:
    """Return `True` when all required fields are present in every record."""
    for record in records:
        for field in fields:
            if is_nullish(record.get(field)):
                return False
    return True
