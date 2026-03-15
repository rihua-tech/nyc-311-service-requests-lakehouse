"""Starter schema conformance helpers."""

from __future__ import annotations

from typing import Any


def missing_fields(record: dict[str, Any], expected_fields: list[str]) -> list[str]:
    """Return fields that are missing from a single record."""
    return [field for field in expected_fields if field not in record]


def schema_matches(record: dict[str, Any], expected_fields: list[str]) -> bool:
    """Return `True` when all expected fields are present."""
    return not missing_fields(record, expected_fields)

