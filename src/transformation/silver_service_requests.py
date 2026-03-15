"""Starter transformations for the silver service request table."""

from __future__ import annotations

import json
import hashlib
from datetime import datetime, timezone
from typing import Any, Iterable

from src.common.utils import parse_optional_datetime, parse_optional_float, utc_now_iso

BoroughValue = str | None


BOROUGH_NORMALIZATION_MAP = {
    "BROOKLYN": "BROOKLYN",
    "BKLYN": "BROOKLYN",
    "BK": "BROOKLYN",
    "BRONX": "BRONX",
    "BX": "BRONX",
    "MANHATTAN": "MANHATTAN",
    "NEW YORK": "MANHATTAN",
    "MN": "MANHATTAN",
    "QUEENS": "QUEENS",
    "QN": "QUEENS",
    "STATEN ISLAND": "STATEN ISLAND",
    "SI": "STATEN ISLAND",
}


def _clean_text(value: Any) -> str | None:
    """Return trimmed text or `None` for blank values."""
    if value in (None, ""):
        return None

    text = str(value).strip()
    return text or None


def _normalize_datetime(value: Any) -> datetime | None:
    """Parse a datetime value and normalize it to UTC when possible."""
    parsed = parse_optional_datetime(value)
    if parsed is None:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def _datetime_to_iso(value: datetime | None) -> str | None:
    """Convert a normalized datetime object to an ISO string."""
    return value.isoformat() if value else None


def _status_is_closed(status_name: str | None, closed_at: datetime | None) -> bool:
    """Return `True` when a request should be treated as closed."""
    normalized_status = (status_name or "").strip().lower()
    return closed_at is not None or normalized_status in {"closed", "resolved"}


def _resolution_time_hours(created_at: datetime | None, closed_at: datetime | None) -> float | None:
    """Calculate resolution time when both timestamps are present."""
    if created_at is None or closed_at is None or closed_at < created_at:
        return None

    return round((closed_at - created_at).total_seconds() / 3600, 2)


def _row_priority_key(record: dict[str, Any]) -> tuple[str, str, str, str]:
    """Build a lightweight ordering key for duplicate request resolution."""
    return (
        record.get("updated_at") or "",
        record.get("closed_at") or "",
        record.get("created_at") or "",
        record.get("load_timestamp") or "",
    )


def parse_raw_payload(raw_payload: str | dict[str, Any]) -> dict[str, Any]:
    """Parse a bronze raw payload into a dictionary."""
    if isinstance(raw_payload, dict):
        return raw_payload

    if not raw_payload:
        return {}

    payload = json.loads(raw_payload)
    if not isinstance(payload, dict):
        raise ValueError("Expected bronze raw_payload to deserialize into a JSON object.")

    return payload


def _fallback_record_hash(payload: dict[str, Any]) -> str:
    """Create a deterministic hash when bronze metadata does not provide one."""
    raw_payload = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(raw_payload.encode("utf-8")).hexdigest()


def parse_bronze_record(bronze_record: dict[str, Any]) -> dict[str, Any]:
    """Parse a bronze row into source payload plus lightweight lineage metadata."""
    payload = parse_raw_payload(bronze_record.get("raw_payload", {}))
    return {
        "payload": payload,
        "source_record_id": bronze_record.get("source_record_id"),
        "record_hash": bronze_record.get("record_hash"),
        "ingest_timestamp": bronze_record.get("ingest_timestamp"),
        "api_pull_timestamp": bronze_record.get("api_pull_timestamp"),
        "file_path": bronze_record.get("file_path"),
    }


def parse_bronze_records(bronze_records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Parse a collection of bronze rows into payload-bearing records."""
    return [parse_bronze_record(record) for record in bronze_records]


def select_core_fields(parsed_bronze_record: dict[str, Any], load_timestamp: str) -> dict[str, Any]:
    """Select the core business fields that belong in the silver table."""
    payload = parsed_bronze_record["payload"]
    request_id = _clean_text(payload.get("unique_key")) or _clean_text(parsed_bronze_record.get("source_record_id"))

    return {
        "request_id": request_id,
        "created_at": payload.get("created_date"),
        "closed_at": payload.get("closed_date"),
        "updated_at": payload.get("resolution_action_updated_date") or payload.get("updated_date"),
        "due_date": payload.get("due_date"),
        "agency_code": _clean_text(payload.get("agency")),
        "agency_name": _clean_text(payload.get("agency_name")) or _clean_text(payload.get("agency")),
        "complaint_type": _clean_text(payload.get("complaint_type")),
        "descriptor": _clean_text(payload.get("descriptor")),
        "status_name": _clean_text(payload.get("status")),
        "borough": payload.get("borough"),
        "incident_zip": _clean_text(payload.get("incident_zip")),
        "city": _clean_text(payload.get("city")),
        "latitude": parse_optional_float(payload.get("latitude")),
        "longitude": parse_optional_float(payload.get("longitude")),
        "location_type": _clean_text(payload.get("location_type")),
        "channel_type": _clean_text(payload.get("open_data_channel_type")),
        "resolution_description": _clean_text(payload.get("resolution_description")),
        "record_hash": parsed_bronze_record.get("record_hash") or _fallback_record_hash(payload),
        "load_timestamp": load_timestamp,
    }


def cast_timestamp_fields(record: dict[str, Any]) -> dict[str, Any]:
    """Cast the table timestamp fields to normalized ISO values."""
    casted = dict(record)
    for field in ("created_at", "closed_at", "updated_at", "due_date", "load_timestamp"):
        casted[field] = _datetime_to_iso(_normalize_datetime(casted.get(field)))
    return casted


def normalize_borough(value: Any) -> BoroughValue:
    """Normalize borough strings to a small canonical set."""
    text = _clean_text(value)
    if text is None:
        return None

    normalized = " ".join(text.upper().split())
    return BOROUGH_NORMALIZATION_MAP.get(normalized, normalized)


def normalize_borough_fields(record: dict[str, Any]) -> dict[str, Any]:
    """Apply borough normalization to a silver row."""
    normalized = dict(record)
    normalized["borough"] = normalize_borough(record.get("borough"))
    return normalized


def derive_date_parts(record: dict[str, Any]) -> dict[str, Any]:
    """Derive calendar columns from the request timestamps."""
    derived = dict(record)
    created_at = _normalize_datetime(record.get("created_at"))
    closed_at = _normalize_datetime(record.get("closed_at"))

    derived["created_year"] = created_at.year if created_at else None
    derived["created_month"] = created_at.month if created_at else None
    derived["created_date"] = created_at.date().isoformat() if created_at else None
    derived["closed_date"] = closed_at.date().isoformat() if closed_at else None
    return derived


def derive_operational_fields(record: dict[str, Any]) -> dict[str, Any]:
    """Derive starter operational fields used by the silver table."""
    derived = dict(record)
    created_at = _normalize_datetime(record.get("created_at"))
    closed_at = _normalize_datetime(record.get("closed_at"))
    due_date = _normalize_datetime(record.get("due_date"))
    load_timestamp = _normalize_datetime(record.get("load_timestamp"))

    is_closed = _status_is_closed(_clean_text(record.get("status_name")), closed_at)
    derived["is_closed"] = is_closed
    derived["is_overdue"] = bool(due_date and load_timestamp and not is_closed and due_date < load_timestamp)
    derived["resolution_time_hours"] = _resolution_time_hours(created_at, closed_at)
    return derived


def transform_bronze_record_to_silver(bronze_record: dict[str, Any], load_timestamp: str) -> dict[str, Any]:
    """Transform one bronze row into a silver-ready record."""
    parsed_record = parse_bronze_record(bronze_record)
    selected_record = select_core_fields(parsed_record, load_timestamp=load_timestamp)
    casted_record = cast_timestamp_fields(selected_record)
    normalized_record = normalize_borough_fields(casted_record)
    dated_record = derive_date_parts(normalized_record)
    return derive_operational_fields(dated_record)


def remove_duplicate_request_ids(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep one silver row per request_id using the latest known timestamp fields."""
    deduplicated: dict[str, dict[str, Any]] = {}
    rows_without_request_id: list[dict[str, Any]] = []

    for record in records:
        request_id = record.get("request_id")
        if request_id in (None, ""):
            rows_without_request_id.append(record)
            continue

        existing = deduplicated.get(str(request_id))
        if existing is None or _row_priority_key(record) >= _row_priority_key(existing):
            deduplicated[str(request_id)] = record

    return list(deduplicated.values()) + rows_without_request_id


def clean_service_request(bronze_record: dict[str, Any]) -> dict[str, Any]:
    """Transform a single bronze record into the silver request schema."""
    return transform_bronze_record_to_silver(bronze_record, load_timestamp=utc_now_iso())


def clean_service_requests(bronze_records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Transform bronze rows into deduplicated silver request records."""
    load_timestamp = utc_now_iso()
    silver_rows = [
        transform_bronze_record_to_silver(bronze_record, load_timestamp=load_timestamp)
        for bronze_record in bronze_records
    ]
    return remove_duplicate_request_ids(silver_rows)
