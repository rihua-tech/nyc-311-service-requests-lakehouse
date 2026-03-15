from src.quality.rowcount_checks import (
    layer_reconciliation_summaries,
    reconciliation_summary,
    rowcount_delta,
    within_tolerance,
)


def test_rowcount_delta_and_tolerance_are_simple() -> None:
    assert rowcount_delta(10, 8) == -2
    assert within_tolerance(10, 8, tolerance=2) is True
    assert within_tolerance(10, 8, tolerance=1) is False


def test_reconciliation_helpers_build_summary_output() -> None:
    summary = reconciliation_summary(
        expected_count=10,
        actual_count=9,
        tolerance=1,
        comparison_name="bronze_to_silver",
    )

    layer_summaries = layer_reconciliation_summaries(
        {"bronze": 10, "silver": 9, "gold_fact": 9},
        baseline_layer="bronze",
        tolerance=1,
    )

    assert summary == {
        "comparison_name": "bronze_to_silver",
        "expected_count": 10,
        "actual_count": 9,
        "delta": -1,
        "within_tolerance": True,
    }
    assert layer_summaries[0]["comparison_name"] == "bronze_to_silver"
    assert layer_summaries[1]["comparison_name"] == "bronze_to_gold_fact"
