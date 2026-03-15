"""Starter builders for gold dimension tables."""

from __future__ import annotations

from datetime import date
from typing import Any, Iterable

from src.common.utils import utc_now_iso


def build_dim_date(values: Iterable[str]) -> list[dict[str, Any]]:
    """Create a minimal date dimension from ISO date values."""
    rows: list[dict[str, Any]] = []
    for value in sorted({item for item in values if item}):
        parsed = date.fromisoformat(value)
        rows.append(
            {
                "date_key": int(parsed.strftime("%Y%m%d")),
                "calendar_date": parsed.isoformat(),
                "calendar_year": parsed.year,
                "calendar_month": parsed.month,
                "calendar_day": parsed.day,
            }
        )
    return rows


def build_named_dimension(
    records: Iterable[dict[str, Any]],
    key_field: str,
    name_field: str,
    surrogate_key_field: str,
) -> list[dict[str, Any]]:
    """Assign simple surrogate keys to unique business keys."""
    unique_rows: dict[Any, dict[str, Any]] = {}
    for record in records:
        business_key = record.get(key_field)
        name = record.get(name_field)
        if business_key not in (None, "") and business_key not in unique_rows:
            unique_rows[business_key] = {
                surrogate_key_field: len(unique_rows) + 1,
                key_field: business_key,
                name_field: name,
                "effective_timestamp": utc_now_iso(),
            }
    return list(unique_rows.values())


def build_dim_agency(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Create starter rows for `gold.dim_agency`."""
    return build_named_dimension(records, "agency_code", "agency_name", "agency_sk")


def build_dim_complaint_type(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Create starter rows for `gold.dim_complaint_type`."""
    return build_named_dimension(records, "complaint_type", "descriptor", "complaint_type_sk")


def build_dim_status(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Create starter rows for `gold.dim_status`."""
    rows = build_named_dimension(records, "status_name", "status_name", "status_sk")
    for row in rows:
        row["status_group"] = None
    return rows


def build_dim_location(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Create starter rows for `gold.dim_location`."""
    unique_rows: dict[tuple[Any, Any, Any], dict[str, Any]] = {}
    for record in records:
        business_key = (
            record.get("incident_zip"),
            record.get("borough"),
            record.get("city"),
        )
        if business_key[0] not in (None, "") and business_key not in unique_rows:
            unique_rows[business_key] = {
                "location_sk": len(unique_rows) + 1,
                "incident_zip": business_key[0],
                "borough": business_key[1],
                "city": business_key[2],
                "effective_timestamp": utc_now_iso(),
            }
    return list(unique_rows.values())
