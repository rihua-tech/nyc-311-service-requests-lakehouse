"""Starter aggregations for gold reporting marts."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timezone
from typing import Any, Iterable


def _clean_text(value: Any, default: str = "<unknown>") -> str:
    """Return trimmed text or a default label for blank values."""
    if value in (None, ""):
        return default

    text = str(value).strip()
    return text or default


def _parse_date(value: Any) -> date | None:
    """Parse an ISO date string into a `date` object."""
    if value in (None, ""):
        return None
    return date.fromisoformat(str(value))


def _resolve_snapshot_date(snapshot_date: str | None = None) -> str:
    """Resolve a snapshot date, defaulting to the current UTC date."""
    if snapshot_date:
        return snapshot_date
    return datetime.now(timezone.utc).date().isoformat()


def _sort_rows(rows: Iterable[dict[str, Any]], fields: list[str]) -> list[dict[str, Any]]:
    """Return deterministically sorted mart rows."""
    return sorted(
        rows,
        key=lambda row: tuple("" if row.get(field) is None else str(row.get(field)) for field in fields),
    )


def _open_requests_as_of_snapshot(
    fact_rows: Iterable[dict[str, Any]],
    snapshot_date: str,
) -> list[dict[str, Any]]:
    """Return fact rows that are still open as of the chosen snapshot date."""
    snapshot_day = _parse_date(snapshot_date)
    if snapshot_day is None:
        raise ValueError("snapshot_date must be a valid ISO date string")

    open_rows: list[dict[str, Any]] = []
    for row in fact_rows:
        created_date = _parse_date(row.get("created_date"))
        closed_date = _parse_date(row.get("closed_date"))
        is_closed = bool(row.get("is_closed"))

        if created_date and created_date > snapshot_day:
            continue

        if is_closed and closed_date and closed_date <= snapshot_day:
            continue

        if (not is_closed) or (closed_date is not None and closed_date > snapshot_day):
            open_rows.append(row)

    return open_rows


def build_request_volume_daily(fact_rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Aggregate daily request counts by created date."""
    counts: dict[str, int] = defaultdict(int)
    for row in fact_rows:
        created_date = row.get("created_date")
        if created_date:
            counts[str(created_date)] += 1

    rows = [{"created_date": key, "request_count": value} for key, value in counts.items()]
    return _sort_rows(rows, ["created_date"])


def build_service_performance(fact_rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Aggregate closed request performance by agency and complaint type."""
    stats: dict[tuple[str, str], dict[str, float]] = defaultdict(
        lambda: {"closed_requests": 0, "timed_request_count": 0, "total_hours": 0.0}
    )
    for row in fact_rows:
        if not row.get("is_closed"):
            continue

        agency_code = _clean_text(row.get("agency_code"))
        complaint_type = _clean_text(row.get("complaint_type"))
        key = (agency_code, complaint_type)

        stats[key]["closed_requests"] += 1

        resolution_time_hours = row.get("resolution_time_hours")
        if resolution_time_hours not in (None, "") and float(resolution_time_hours) >= 0:
            stats[key]["timed_request_count"] += 1
            stats[key]["total_hours"] += float(resolution_time_hours)

    output: list[dict[str, Any]] = []
    for (agency_code, complaint_type), values in stats.items():
        closed_requests = int(values["closed_requests"])
        timed_request_count = int(values["timed_request_count"])
        avg_resolution = (
            round(values["total_hours"] / timed_request_count, 2)
            if timed_request_count
            else None
        )
        output.append(
            {
                "agency_code": agency_code,
                "complaint_type": complaint_type,
                "closed_requests": closed_requests,
                "avg_resolution_time_hours": avg_resolution,
            }
        )
    return _sort_rows(output, ["agency_code", "complaint_type"])


def build_backlog_snapshot(
    fact_rows: Iterable[dict[str, Any]],
    snapshot_date: str | None = None,
) -> list[dict[str, Any]]:
    """Aggregate open request counts by status and agency for a snapshot date."""
    resolved_snapshot_date = _resolve_snapshot_date(snapshot_date)
    counts: dict[tuple[str, str], int] = defaultdict(int)
    for row in _open_requests_as_of_snapshot(fact_rows, resolved_snapshot_date):
        status_name = _clean_text(row.get("status_name"))
        agency_code = _clean_text(row.get("agency_code"))
        counts[(status_name, agency_code)] += 1

    rows = [
        {
            "snapshot_date": resolved_snapshot_date,
            "status_name": status_name,
            "agency_code": agency_code,
            "open_request_count": value,
        }
        for (status_name, agency_code), value in counts.items()
    ]
    return _sort_rows(rows, ["snapshot_date", "status_name", "agency_code"])
