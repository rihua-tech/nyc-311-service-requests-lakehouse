"""Starter row-count validation helpers."""

from __future__ import annotations


def rowcount_delta(expected_count: int, actual_count: int) -> int:
    """Return the signed difference between expected and actual counts."""
    return actual_count - expected_count


def within_tolerance(expected_count: int, actual_count: int, tolerance: int = 0) -> bool:
    """Return `True` when row-count variance stays within a threshold."""
    return abs(rowcount_delta(expected_count, actual_count)) <= tolerance
