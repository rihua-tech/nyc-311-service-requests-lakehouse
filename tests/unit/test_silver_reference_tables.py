from src.transformation.silver_reference_tables import (
    build_agency_reference,
    build_complaint_type_reference,
    build_location_reference,
    build_status_reference,
)


def test_reference_builders_keep_earliest_created_date() -> None:
    rows = [
        {
            "agency_code": "NYPD",
            "agency_name": "Police Department",
            "complaint_type": "Noise",
            "descriptor": "Loud Music",
            "created_date": "2024-01-03",
        },
        {
            "agency_code": "NYPD",
            "agency_name": "Police Department",
            "complaint_type": "Noise",
            "descriptor": "Loud Music",
            "created_date": "2024-01-01",
        }
    ]

    agency_rows = build_agency_reference(rows)
    reference_rows = build_complaint_type_reference(rows)

    assert agency_rows[0]["first_seen_created_date"] == "2024-01-01"
    assert reference_rows[0]["first_seen_created_date"] == "2024-01-01"


def test_build_location_reference_includes_location_fields() -> None:
    rows = [
        {
            "incident_zip": "11201",
            "borough": "BROOKLYN",
            "city": "BROOKLYN",
            "location_type": "Street",
            "latitude": 40.0,
            "longitude": -73.0,
        }
    ]

    reference_rows = build_location_reference(rows)

    assert reference_rows[0]["location_type"] == "Street"
    assert reference_rows[0]["latitude"] == 40.0


def test_build_status_reference_derives_group_and_flag() -> None:
    rows = [{"status_name": "Closed"}]

    reference_rows = build_status_reference(rows)

    assert reference_rows[0]["status_group"] == "Closed"
    assert reference_rows[0]["is_closed_status"] is True
