"""Starter builders for the gold fact table."""

from __future__ import annotations

from datetime import date
from typing import Any, Iterable

from src.transformation.gold_dimensions import (
    build_dim_agency,
    build_dim_complaint_type,
    build_dim_location,
    build_dim_status,
)

UNKNOWN_SURROGATE_KEY = 0


def _clean_text(value: Any) -> str | None:
    """Return trimmed text or `None` for blank values."""
    if value in (None, ""):
        return None

    text = str(value).strip()
    return text or None


def _build_business_key(record: dict[str, Any], fields: list[str]) -> tuple[Any, ...]:
    """Build a normalized business key tuple from a record."""
    return tuple(_clean_text(record.get(field)) for field in fields)


def build_date_key(date_value: Any) -> int | None:
    """Convert an ISO date string to a YYYYMMDD integer key."""
    if date_value in (None, ""):
        return None

    parsed = date.fromisoformat(str(date_value))
    return int(parsed.strftime("%Y%m%d"))


def build_dimension_lookup(
    dimension_rows: Iterable[dict[str, Any]],
    business_fields: list[str],
    surrogate_key_field: str,
) -> dict[tuple[Any, ...], int]:
    """Create a lookup from business keys to surrogate keys."""
    lookup: dict[tuple[Any, ...], int] = {}
    for row in dimension_rows:
        lookup[_build_business_key(row, business_fields)] = int(row.get(surrogate_key_field, UNKNOWN_SURROGATE_KEY))
    return lookup


def resolve_surrogate_key(
    record: dict[str, Any],
    business_fields: list[str],
    lookup: dict[tuple[Any, ...], int],
) -> int:
    """Resolve a surrogate key from a record and a prepared lookup."""
    return lookup.get(_build_business_key(record, business_fields), UNKNOWN_SURROGATE_KEY)


def build_fact_service_request(
    silver_record: dict[str, Any],
    agency_lookup: dict[tuple[Any, ...], int],
    complaint_type_lookup: dict[tuple[Any, ...], int],
    location_lookup: dict[tuple[Any, ...], int],
    status_lookup: dict[tuple[Any, ...], int],
) -> dict[str, Any]:
    """Build one request-level fact row from a silver request record."""
    return {
        "request_id": silver_record.get("request_id"),
        "created_date_key": build_date_key(silver_record.get("created_date")),
        "closed_date_key": build_date_key(silver_record.get("closed_date")),
        "agency_sk": resolve_surrogate_key(silver_record, ["agency_code"], agency_lookup),
        "complaint_type_sk": resolve_surrogate_key(
            silver_record,
            ["complaint_type", "descriptor"],
            complaint_type_lookup,
        ),
        "location_sk": resolve_surrogate_key(
            silver_record,
            ["incident_zip", "borough", "city", "location_type"],
            location_lookup,
        ),
        "status_sk": resolve_surrogate_key(silver_record, ["status_name"], status_lookup),
        "created_at": silver_record.get("created_at"),
        "closed_at": silver_record.get("closed_at"),
        "due_date": silver_record.get("due_date"),
        "created_date": silver_record.get("created_date"),
        "closed_date": silver_record.get("closed_date"),
        "agency_code": silver_record.get("agency_code"),
        "complaint_type": silver_record.get("complaint_type"),
        "descriptor": silver_record.get("descriptor"),
        "status_name": silver_record.get("status_name"),
        "incident_zip": silver_record.get("incident_zip"),
        "borough": silver_record.get("borough"),
        "city": silver_record.get("city"),
        "location_type": silver_record.get("location_type"),
        "channel_type": silver_record.get("channel_type"),
        "resolution_time_hours": silver_record.get("resolution_time_hours"),
        "is_closed": silver_record.get("is_closed"),
        "is_overdue": silver_record.get("is_overdue"),
        "record_hash": silver_record.get("record_hash"),
        "load_timestamp": silver_record.get("load_timestamp"),
    }


def build_fact_service_requests(
    records: Iterable[dict[str, Any]],
    dim_agency_rows: Iterable[dict[str, Any]] | None = None,
    dim_complaint_type_rows: Iterable[dict[str, Any]] | None = None,
    dim_location_rows: Iterable[dict[str, Any]] | None = None,
    dim_status_rows: Iterable[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Build the gold request-level fact table from silver rows and dimension rows."""
    silver_rows = list(records)

    agency_rows = list(dim_agency_rows) if dim_agency_rows is not None else build_dim_agency(silver_rows)
    complaint_type_rows = (
        list(dim_complaint_type_rows)
        if dim_complaint_type_rows is not None
        else build_dim_complaint_type(silver_rows)
    )
    location_rows = list(dim_location_rows) if dim_location_rows is not None else build_dim_location(silver_rows)
    status_rows = list(dim_status_rows) if dim_status_rows is not None else build_dim_status(silver_rows)

    agency_lookup = build_dimension_lookup(agency_rows, ["agency_code"], "agency_sk")
    complaint_type_lookup = build_dimension_lookup(
        complaint_type_rows,
        ["complaint_type", "descriptor"],
        "complaint_type_sk",
    )
    location_lookup = build_dimension_lookup(
        location_rows,
        ["incident_zip", "borough", "city", "location_type"],
        "location_sk",
    )
    status_lookup = build_dimension_lookup(status_rows, ["status_name"], "status_sk")

    return [
        build_fact_service_request(
            silver_record=record,
            agency_lookup=agency_lookup,
            complaint_type_lookup=complaint_type_lookup,
            location_lookup=location_lookup,
            status_lookup=status_lookup,
        )
        for record in silver_rows
    ]
