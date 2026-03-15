from src.transformation.silver_reference_tables import (
    build_complaint_type_reference,
    build_location_reference,
    build_status_reference,
)


def test_build_complaint_type_reference_keeps_first_seen_date() -> None:
    rows = [
        {
            "complaint_type": "Noise",
            "descriptor": "Loud Music",
            "created_date": "2024-01-01",
        }
    ]

    reference_rows = build_complaint_type_reference(rows)

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
