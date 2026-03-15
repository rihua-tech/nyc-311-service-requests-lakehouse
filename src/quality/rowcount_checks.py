"""Starter row-count validation helpers."""

from __future__ import annotations

from collections.abc import Mapping


def rowcount_delta(expected_count: int, actual_count: int) -> int:
    """Return the signed difference between expected and actual counts."""
    return actual_count - expected_count


def within_tolerance(expected_count: int, actual_count: int, tolerance: int = 0) -> bool:
    """Return `True` when row-count variance stays within a threshold."""
    return abs(rowcount_delta(expected_count, actual_count)) <= tolerance


def reconciliation_summary(
    expected_count: int,
    actual_count: int,
    tolerance: int = 0,
    comparison_name: str = "rowcount_reconciliation",
) -> dict[str, int | bool | str]:
    """Build a compact reconciliation summary."""
    return {
        "comparison_name": comparison_name,
        "expected_count": expected_count,
        "actual_count": actual_count,
        "delta": rowcount_delta(expected_count, actual_count),
        "within_tolerance": within_tolerance(expected_count, actual_count, tolerance=tolerance),
    }


def layer_reconciliation_summaries(
    layer_counts: Mapping[str, int],
    baseline_layer: str,
    tolerance: int = 0,
) -> list[dict[str, int | bool | str]]:
    """Compare each layer count to a chosen baseline layer."""
    baseline_count = layer_counts[baseline_layer]
    summaries: list[dict[str, int | bool | str]] = []
    for layer_name, layer_count in layer_counts.items():
        if layer_name == baseline_layer:
            continue
        summaries.append(
            reconciliation_summary(
                expected_count=baseline_count,
                actual_count=layer_count,
                tolerance=tolerance,
                comparison_name=f"{baseline_layer}_to_{layer_name}",
            )
        )
    return summaries
