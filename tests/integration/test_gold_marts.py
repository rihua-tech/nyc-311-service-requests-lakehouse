from src.transformation.gold_facts import build_fact_service_requests
from src.transformation.gold_marts import (
    build_backlog_snapshot,
    build_request_volume_daily,
    build_service_performance,
)


def test_build_request_volume_daily_counts_rows_by_date() -> None:
    silver_rows = [
        {"request_id": "1", "created_date": "2024-01-01", "is_closed": True},
        {"request_id": "2", "created_date": "2024-01-01", "is_closed": False},
    ]

    fact_rows = build_fact_service_requests(silver_rows)
    mart_rows = build_request_volume_daily(fact_rows)

    assert mart_rows == [{"created_date": "2024-01-01", "request_count": 2}]


def test_build_service_performance_and_backlog_snapshot_return_simple_outputs() -> None:
    fact_rows = [
        {
            "request_id": "1",
            "agency_code": "DSNY",
            "status_name": "Closed",
            "resolution_time_hours": 4.0,
            "is_closed": True,
        },
        {
            "request_id": "2",
            "agency_code": "DEP",
            "status_name": "Open",
            "resolution_time_hours": None,
            "is_closed": False,
        },
    ]

    performance_rows = build_service_performance(fact_rows)
    backlog_rows = build_backlog_snapshot(fact_rows)

    assert performance_rows[0]["closed_requests"] == 1
    assert backlog_rows == [{"status_name": "Open", "open_request_count": 1}]

