from src.transformation.gold_marts import (
    build_backlog_snapshot,
    build_request_volume_daily,
    build_service_performance,
)


def test_build_request_volume_daily_sorts_by_created_date() -> None:
    fact_rows = [
        {"created_date": "2024-01-02"},
        {"created_date": "2024-01-01"},
        {"created_date": "2024-01-01"},
    ]

    mart_rows = build_request_volume_daily(fact_rows)

    assert mart_rows == [
        {"created_date": "2024-01-01", "request_count": 2},
        {"created_date": "2024-01-02", "request_count": 1},
    ]


def test_build_service_performance_groups_by_agency_and_complaint_type() -> None:
    fact_rows = [
        {
            "agency_code": "DSNY",
            "complaint_type": "Missed Collection",
            "resolution_time_hours": 2.0,
            "is_closed": True,
        },
        {
            "agency_code": "DSNY",
            "complaint_type": "Missed Collection",
            "resolution_time_hours": 4.0,
            "is_closed": True,
        },
        {
            "agency_code": "DSNY",
            "complaint_type": "Missed Collection",
            "resolution_time_hours": -1.0,
            "is_closed": True,
        },
        {
            "agency_code": "DEP",
            "complaint_type": "Water System",
            "resolution_time_hours": None,
            "is_closed": False,
        },
    ]

    mart_rows = build_service_performance(fact_rows)

    assert mart_rows == [
        {
            "agency_code": "DSNY",
            "complaint_type": "Missed Collection",
            "closed_requests": 3,
            "avg_resolution_time_hours": 3.0,
        }
    ]


def test_build_backlog_snapshot_uses_snapshot_date_logic() -> None:
    fact_rows = [
        {
            "request_id": "1001",
            "created_date": "2024-01-01",
            "closed_date": None,
            "status_name": "Open",
            "agency_code": "DEP",
            "is_closed": False,
        },
        {
            "request_id": "1002",
            "created_date": "2024-01-03",
            "closed_date": None,
            "status_name": "Open",
            "agency_code": "DEP",
            "is_closed": False,
        },
        {
            "request_id": "1003",
            "created_date": "2024-01-01",
            "closed_date": "2024-01-02",
            "status_name": "Closed",
            "agency_code": "DSNY",
            "is_closed": True,
        },
    ]

    mart_rows = build_backlog_snapshot(fact_rows, snapshot_date="2024-01-02")

    assert mart_rows == [
        {
            "snapshot_date": "2024-01-02",
            "status_name": "Open",
            "agency_code": "DEP",
            "open_request_count": 1,
        }
    ]
