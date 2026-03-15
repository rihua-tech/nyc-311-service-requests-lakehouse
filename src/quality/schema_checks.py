"""Starter schema conformance helpers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

ExpectedType = type | tuple[type, ...]


def missing_fields(record: dict[str, Any], expected_fields: list[str]) -> list[str]:
    """Return fields that are missing from a single record."""
    return [field for field in expected_fields if field not in record]


def unexpected_fields(record: dict[str, Any], expected_fields: list[str]) -> list[str]:
    """Return fields that are not part of the expected schema."""
    return [field for field in record if field not in expected_fields]


def schema_matches(record: dict[str, Any], expected_fields: list[str]) -> bool:
    """Return `True` when all expected fields are present."""
    return not missing_fields(record, expected_fields)


def type_mismatches(
    record: dict[str, Any],
    expected_types: Mapping[str, ExpectedType],
) -> dict[str, str]:
    """Return field-level type mismatches for simple runtime checks."""
    mismatches: dict[str, str] = {}
    for field, expected_type in expected_types.items():
        value = record.get(field)
        if value is None:
            continue

        if not isinstance(value, expected_type):
            allowed_types = expected_type if isinstance(expected_type, tuple) else (expected_type,)
            mismatches[field] = ", ".join(sorted(type_.__name__ for type_ in allowed_types))
    return mismatches


def has_expected_types(record: dict[str, Any], expected_types: Mapping[str, ExpectedType]) -> bool:
    """Return `True` when a record satisfies the starter type expectations."""
    return not type_mismatches(record, expected_types)
