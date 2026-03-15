from src.transformation.gold_dimensions import (
    build_dim_agency,
    build_dim_complaint_type,
    build_dim_date,
    build_dim_location,
    build_dim_status,
    collect_date_values,
)


def test_collect_date_values_gathers_created_and_closed_dates() -> None:
    rows = [
        {"created_date": "2024-01-02", "closed_date": "2024-01-03"},
        {"created_date": "2024-01-01", "closed_date": None},
    ]

    assert collect_date_values(rows) == ["2024-01-01", "2024-01-02", "2024-01-03"]


def test_build_dim_date_derives_calendar_attributes() -> None:
    rows = build_dim_date(["2024-01-06"])

    assert rows == [
        {
            "date_key": 20240106,
            "calendar_date": "2024-01-06",
            "calendar_year": 2024,
            "calendar_quarter": 1,
            "calendar_month": 1,
            "month_name": "January",
            "calendar_day": 6,
            "day_of_week": 6,
            "day_name": "Saturday",
            "is_weekend": True,
        }
    ]


def test_build_dimension_helpers_assign_stable_surrogate_keys() -> None:
    agency_rows = build_dim_agency(
        [
            {"agency_code": "DSNY", "agency_name": "Department of Sanitation"},
            {"agency_code": "DEP", "agency_name": "Department of Environmental Protection"},
        ]
    )
    complaint_rows = build_dim_complaint_type(
        [
            {"complaint_type": "Water System", "descriptor": "Leak"},
            {"complaint_type": "Noise", "descriptor": "Loud Music"},
        ]
    )
    location_rows = build_dim_location(
        [
            {
                "incident_zip": "11201",
                "borough": "BROOKLYN",
                "city": "BROOKLYN",
                "location_type": "Residential Building",
                "latitude": 40.6931,
                "longitude": -73.9897,
            }
        ]
    )
    status_rows = build_dim_status([{"status_name": "Closed"}])

    assert agency_rows[0]["agency_code"] == "DEP"
    assert agency_rows[0]["agency_sk"] == 1
    assert complaint_rows[0]["complaint_type"] == "Noise"
    assert complaint_rows[0]["complaint_type_sk"] == 1
    assert location_rows[0]["location_sk"] == 1
    assert status_rows[0]["status_group"] == "Closed"
    assert status_rows[0]["is_closed_status"] is True

