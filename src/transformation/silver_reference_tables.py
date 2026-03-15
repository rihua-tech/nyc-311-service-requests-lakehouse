"""Starter builders for silver reference tables."""

from __future__ import annotations

from typing import Any, Iterable

from src.common.utils import utc_now_iso


def _clean_text(value: Any) -> str | None:
    """Return trimmed text or `None` for blank values."""
    if value in (None, ""):
        return None

    text = str(value).strip()
    return text or None


def _unique_reference_rows(
    records: Iterable[dict[str, Any]],
    business_fields: list[str],
    extra_fields: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Extract unique business-key rows from standardized records."""
    selected_fields = business_fields + (extra_fields or [])
    seen: dict[tuple[Any, ...], dict[str, Any]] = {}
    load_timestamp = utc_now_iso()

    for record in records:
        key = tuple(_clean_text(record.get(field)) if isinstance(record.get(field), str) else record.get(field) for field in business_fields)
        if key and key[0] not in (None, "") and key not in seen:
            seen[key] = {
                field: _clean_text(record.get(field)) if isinstance(record.get(field), str) else record.get(field)
                for field in selected_fields
            }
            seen[key]["load_timestamp"] = load_timestamp
    return list(seen.values())


def build_agency_reference(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Create starter rows for `silver.agency_reference`."""
    rows = _unique_reference_rows(records, ["agency_code", "agency_name"], ["created_date"])
    for row in rows:
        row["first_seen_created_date"] = row.pop("created_date", None)
    return rows


def build_complaint_type_reference(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Create starter rows for `silver.complaint_type_reference`."""
    rows = _unique_reference_rows(records, ["complaint_type", "descriptor"], ["created_date"])
    for row in rows:
        row["first_seen_created_date"] = row.pop("created_date", None)
    return rows


def build_location_reference(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Create starter rows for `silver.location_reference`."""
    return _unique_reference_rows(
        records,
        ["incident_zip", "borough", "city", "location_type"],
        ["latitude", "longitude"],
    )


def build_status_reference(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Create starter rows for `silver.status_reference`."""
    status_rows = _unique_reference_rows(records, ["status_name"])
    for row in status_rows:
        is_closed_status = bool(row["status_name"] and str(row["status_name"]).lower() in {"closed", "resolved"})
        row["status_group"] = "Closed" if is_closed_status else "Open"
        row["is_closed_status"] = is_closed_status
    return status_rows
