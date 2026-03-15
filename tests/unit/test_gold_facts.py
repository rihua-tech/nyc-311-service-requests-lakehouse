from src.transformation.gold_dimensions import (
    build_dim_agency,
    build_dim_complaint_type,
    build_dim_location,
    build_dim_status,
)
from src.transformation.gold_facts import (
    UNKNOWN_SURROGATE_KEY,
    build_date_key,
    build_fact_service_requests,
)


def test_build_date_key_converts_iso_dates_to_integer_keys() -> None:
    assert build_date_key("2024-01-02") == 20240102
    assert build_date_key(None) is None


def test_build_fact_service_requests_resolves_surrogate_keys() -> None:
    silver_rows = [
        {
            "request_id": "1001",
            "created_at": "2024-01-01T10:00:00+00:00",
            "closed_at": "2024-01-01T12:00:00+00:00",
            "due_date": "2024-01-01T18:00:00+00:00",
            "created_date": "2024-01-01",
            "closed_date": "2024-01-01",
            "agency_code": "DSNY",
            "agency_name": "Department of Sanitation",
            "complaint_type": "Missed Collection",
            "descriptor": "Collection missed",
            "status_name": "Closed",
            "incident_zip": "11201",
            "borough": "BROOKLYN",
            "city": "BROOKLYN",
            "location_type": "Residential Building",
            "latitude": 40.6931,
            "longitude": -73.9897,
            "channel_type": "Phone",
            "resolution_time_hours": 2.0,
            "is_closed": True,
            "is_overdue": False,
            "record_hash": "hash-1001",
            "load_timestamp": "2024-01-01T13:00:00+00:00",
        }
    ]

    fact_rows = build_fact_service_requests(
        silver_rows,
        dim_agency_rows=build_dim_agency(silver_rows),
        dim_complaint_type_rows=build_dim_complaint_type(silver_rows),
        dim_location_rows=build_dim_location(silver_rows),
        dim_status_rows=build_dim_status(silver_rows),
    )

    assert fact_rows[0]["request_id"] == "1001"
    assert fact_rows[0]["created_date_key"] == 20240101
    assert fact_rows[0]["agency_sk"] == 1
    assert fact_rows[0]["complaint_type_sk"] == 1
    assert fact_rows[0]["location_sk"] == 1
    assert fact_rows[0]["status_sk"] == 1


def test_build_fact_service_requests_uses_unknown_key_when_dimension_missing() -> None:
    silver_rows = [
        {
            "request_id": "1002",
            "created_date": "2024-01-02",
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
        }
    ]

    fact_rows = build_fact_service_requests(
        silver_rows,
        dim_agency_rows=[],
        dim_complaint_type_rows=[],
        dim_location_rows=[],
        dim_status_rows=[],
    )

    assert fact_rows[0]["agency_sk"] == UNKNOWN_SURROGATE_KEY
    assert fact_rows[0]["status_sk"] == UNKNOWN_SURROGATE_KEY
