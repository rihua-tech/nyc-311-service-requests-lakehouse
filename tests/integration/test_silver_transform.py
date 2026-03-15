from datetime import datetime, timezone

from src.ingestion.bronze_loader import build_bronze_record
from src.transformation.silver_reference_tables import (
    build_agency_reference,
    build_complaint_type_reference,
    build_location_reference,
    build_status_reference,
)
from src.transformation.silver_service_requests import clean_service_request, clean_service_requests


def test_clean_service_request_maps_core_fields_from_bronze_row() -> None:
    bronze_row = build_bronze_record(
        {
            "unique_key": "1001",
            "created_date": "2024-01-01T10:00:00",
            "closed_date": "2024-01-01T12:00:00",
            "agency": "DSNY",
            "agency_name": "Department of Sanitation",
            "complaint_type": "Missed Collection",
            "status": "Closed",
            "borough": "BKLYN",
            "incident_zip": "11201",
        },
        ingest_timestamp=datetime(2024, 1, 1, 13, 0, tzinfo=timezone.utc),
        api_pull_timestamp="2024-01-01T12:30:00+00:00",
    )

    row = clean_service_request(bronze_row)

    assert row["request_id"] == "1001"
    assert row["borough"] == "BROOKLYN"
    assert row["created_year"] == 2024
    assert row["created_month"] == 1
    assert row["created_date"] == "2024-01-01"
    assert row["is_closed"] is True
    assert row["resolution_time_hours"] == 2.0


def test_clean_service_requests_removes_duplicate_request_ids() -> None:
    first_row = build_bronze_record(
        {
            "unique_key": "1001",
            "created_date": "2024-01-01T10:00:00",
            "status": "Open",
            "resolution_action_updated_date": "2024-01-01T11:00:00",
            "agency": "DSNY",
        }
    )
    second_row = build_bronze_record(
        {
            "unique_key": "1001",
            "created_date": "2024-01-01T10:00:00",
            "status": "Closed",
            "closed_date": "2024-01-01T13:00:00",
            "resolution_action_updated_date": "2024-01-01T13:00:00",
            "agency": "DSNY",
        }
    )

    rows = clean_service_requests([first_row, second_row])

    assert len(rows) == 1
    assert rows[0]["is_closed"] is True
    assert rows[0]["closed_date"] == "2024-01-01"


def test_reference_builders_return_starter_rows_from_cleaned_silver_data() -> None:
    cleaned_rows = [
        {
            "agency_code": "DSNY",
            "agency_name": "Department of Sanitation",
            "complaint_type": "Missed Collection",
            "descriptor": "Collection missed",
            "borough": "BROOKLYN",
            "incident_zip": "11201",
            "city": "BROOKLYN",
            "location_type": "Residential Building",
            "latitude": 40.6931,
            "longitude": -73.9897,
            "status_name": "Closed",
            "created_date": "2024-01-01",
        },
        {
            "agency_code": "DSNY",
            "agency_name": "Department of Sanitation",
            "complaint_type": "Missed Collection",
            "descriptor": "Collection missed",
            "borough": "BROOKLYN",
            "incident_zip": "11201",
            "city": "BROOKLYN",
            "location_type": "Residential Building",
            "latitude": 40.6931,
            "longitude": -73.9897,
            "status_name": "Closed",
            "created_date": "2024-01-01",
        },
    ]

    agency_rows = build_agency_reference(cleaned_rows)
    complaint_rows = build_complaint_type_reference(cleaned_rows)
    location_rows = build_location_reference(cleaned_rows)
    status_rows = build_status_reference(cleaned_rows)

    assert len(agency_rows) == 1
    assert agency_rows[0]["first_seen_created_date"] == "2024-01-01"
    assert len(complaint_rows) == 1
    assert len(location_rows) == 1
    assert status_rows[0]["status_group"] == "Closed"
