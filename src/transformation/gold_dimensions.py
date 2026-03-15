"""Starter builders for gold dimension tables."""

from __future__ import annotations

import calendar
from datetime import date
from typing import Any, Iterable

from src.common.utils import utc_now_iso


def _clean_text(value: Any) -> str | None:
    """Return trimmed text or `None` for blank values."""
    if value in (None, ""):
        return None

    text = str(value).strip()
    return text or None


def _normalize_value(value: Any) -> Any:
    """Normalize string values while preserving numeric and boolean types."""
    return _clean_text(value) if isinstance(value, str) else value


def _sorted_unique_rows(
    records: Iterable[dict[str, Any]],
    business_fields: list[str],
    attribute_fields: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Return unique dimension source rows sorted by their business keys."""
    selected_fields = business_fields + (attribute_fields or [])
    unique_rows: dict[tuple[Any, ...], dict[str, Any]] = {}

    for record in records:
        business_key = tuple(_normalize_value(record.get(field)) for field in business_fields)
        if not business_key or business_key[0] in (None, ""):
            continue

        if business_key not in unique_rows:
            unique_rows[business_key] = {
                field: _normalize_value(record.get(field))
                for field in selected_fields
            }

    return [
        unique_rows[key]
        for key in sorted(
            unique_rows,
            key=lambda values: tuple("" if value is None else str(value) for value in values),
        )
    ]


def _assign_surrogate_keys(
    rows: Iterable[dict[str, Any]],
    surrogate_key_field: str,
) -> list[dict[str, Any]]:
    """Assign simple sequential surrogate keys to a set of dimension rows."""
    effective_timestamp = utc_now_iso()
    output: list[dict[str, Any]] = []
    for index, row in enumerate(rows, start=1):
        output.append(
            {
                surrogate_key_field: index,
                **row,
                "effective_timestamp": effective_timestamp,
            }
        )
    return output


def _derive_status_group(status_name: str | None, is_closed_status: bool | None) -> tuple[str | None, bool]:
    """Derive a simple status grouping for the starter gold model."""
    if isinstance(is_closed_status, bool):
        return ("Closed" if is_closed_status else "Open", is_closed_status)

    normalized_status = (status_name or "").strip().lower()
    derived_is_closed = normalized_status in {"closed", "resolved"}
    return ("Closed" if derived_is_closed else "Open", derived_is_closed)


def collect_date_values(
    records: Iterable[dict[str, Any]],
    date_fields: tuple[str, ...] = ("created_date", "closed_date"),
) -> list[str]:
    """Collect distinct non-null date values from a sequence of records."""
    values = {
        str(record.get(field))
        for record in records
        for field in date_fields
        if record.get(field) not in (None, "")
    }
    return sorted(values)


def build_dim_date(values: Iterable[str]) -> list[dict[str, Any]]:
    """Create a date dimension from ISO date values."""
    rows: list[dict[str, Any]] = []
    for value in sorted({item for item in values if item}):
        parsed = date.fromisoformat(value)
        day_of_week = parsed.isoweekday()
        rows.append(
            {
                "date_key": int(parsed.strftime("%Y%m%d")),
                "calendar_date": parsed.isoformat(),
                "calendar_year": parsed.year,
                "calendar_quarter": ((parsed.month - 1) // 3) + 1,
                "calendar_month": parsed.month,
                "month_name": calendar.month_name[parsed.month],
                "calendar_day": parsed.day,
                "day_of_week": day_of_week,
                "day_name": calendar.day_name[day_of_week - 1],
                "is_weekend": day_of_week >= 6,
            }
        )
    return rows


def build_dim_agency(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Create starter rows for `gold.dim_agency`."""
    rows = _sorted_unique_rows(
        records,
        business_fields=["agency_code"],
        attribute_fields=["agency_name", "first_seen_created_date"],
    )
    return _assign_surrogate_keys(rows, "agency_sk")


def build_dim_complaint_type(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Create starter rows for `gold.dim_complaint_type`."""
    rows = _sorted_unique_rows(
        records,
        business_fields=["complaint_type", "descriptor"],
        attribute_fields=["first_seen_created_date"],
    )
    return _assign_surrogate_keys(rows, "complaint_type_sk")


def build_dim_status(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Create starter rows for `gold.dim_status`."""
    base_rows = _sorted_unique_rows(
        records,
        business_fields=["status_name"],
        attribute_fields=["status_group", "is_closed_status"],
    )

    normalized_rows: list[dict[str, Any]] = []
    for row in base_rows:
        status_group, is_closed_status = _derive_status_group(
            row.get("status_name"),
            row.get("is_closed_status"),
        )
        normalized_rows.append(
            {
                "status_name": row.get("status_name"),
                "status_group": row.get("status_group") or status_group,
                "is_closed_status": is_closed_status,
            }
        )

    return _assign_surrogate_keys(normalized_rows, "status_sk")


def build_dim_location(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Create starter rows for `gold.dim_location`."""
    rows = _sorted_unique_rows(
        records,
        business_fields=["incident_zip", "borough", "city", "location_type"],
        attribute_fields=["latitude", "longitude"],
    )
    return _assign_surrogate_keys(rows, "location_sk")
