from src.transformation.gold_facts import build_fact_service_requests
from src.transformation.gold_marts import (
    build_backlog_snapshot,
    build_request_volume_daily,
    build_service_performance,
)


def test_build_request_volume_daily_counts_rows_by_date() -> None:
    silver_rows = [
        {
            "request_id": "1",
            "created_date": "2024-01-01",
            "agency_code": "DSNY",
            "complaint_type": "Missed Collection",
            "descriptor": "Collection missed",
            "status_name": "Closed",
            "incident_zip": "11201",
            "borough": "BROOKLYN",
            "city": "BROOKLYN",
            "location_type": "Residential Building",
            "is_closed": True,
            "is_overdue": False,
        },
        {
            "request_id": "2",
            "created_date": "2024-01-01",
            "agency_code": "DEP",
            "complaint_type": "Water System",
            "descriptor": "Leak",
            "status_name": "Open",
            "incident_zip": "11101",
            "borough": "QUEENS",
            "city": "LONG ISLAND CITY",
            "location_type": "Street",
            "is_closed": False,
            "is_overdue": False,
        },
    ]

    fact_rows = build_fact_service_requests(silver_rows)
    mart_rows = build_request_volume_daily(fact_rows)

    assert mart_rows == [{"created_date": "2024-01-01", "request_count": 2}]


def test_build_service_performance_and_backlog_snapshot_return_simple_outputs() -> None:
    fact_rows = [
        {
            "request_id": "1",
            "agency_code": "DSNY",
            "complaint_type": "Missed Collection",
            "created_date": "2024-01-01",
            "closed_date": "2024-01-01",
            "status_name": "Closed",
            "resolution_time_hours": 4.0,
            "is_closed": True,
        },
        {
            "request_id": "2",
            "agency_code": "DEP",
            "complaint_type": "Water System",
            "created_date": "2024-01-01",
            "closed_date": None,
            "status_name": "Open",
            "resolution_time_hours": None,
            "is_closed": False,
        },
    ]

    performance_rows = build_service_performance(fact_rows)
    backlog_rows = build_backlog_snapshot(fact_rows, snapshot_date="2024-01-01")

    assert performance_rows == [
        {
            "agency_code": "DSNY",
            "complaint_type": "Missed Collection",
            "closed_requests": 1,
            "avg_resolution_time_hours": 4.0,
        }
    ]
    assert backlog_rows == [
        {
            "snapshot_date": "2024-01-01",
            "status_name": "Open",
            "agency_code": "DEP",
            "open_request_count": 1,
        }
    ]
