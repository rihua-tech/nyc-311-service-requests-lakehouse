"""Starter aggregations for gold reporting marts."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable


def build_request_volume_daily(fact_rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Aggregate daily request counts by created date."""
    counts: dict[str, int] = defaultdict(int)
    for row in fact_rows:
        created_date = row.get("created_date")
        if created_date:
            counts[str(created_date)] += 1

    return [{"created_date": key, "request_count": value} for key, value in sorted(counts.items())]


def build_service_performance(fact_rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Aggregate closed request performance by agency."""
    stats: dict[str, dict[str, float]] = defaultdict(lambda: {"closed_requests": 0, "total_hours": 0.0})
    for row in fact_rows:
        agency_code = str(row.get("agency_code") or "<unknown>")
        if row.get("is_closed"):
            stats[agency_code]["closed_requests"] += 1
            stats[agency_code]["total_hours"] += float(row.get("resolution_time_hours") or 0.0)

    output: list[dict[str, Any]] = []
    for agency_code, values in sorted(stats.items()):
        closed_requests = int(values["closed_requests"])
        avg_resolution = round(values["total_hours"] / closed_requests, 2) if closed_requests else None
        output.append(
            {
                "agency_code": agency_code,
                "closed_requests": closed_requests,
                "avg_resolution_time_hours": avg_resolution,
            }
        )
    return output


def build_backlog_snapshot(fact_rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Aggregate open request counts by status."""
    counts: dict[str, int] = defaultdict(int)
    for row in fact_rows:
        if not row.get("is_closed"):
            status_name = str(row.get("status_name") or "<unknown>")
            counts[status_name] += 1

    return [{"status_name": key, "open_request_count": value} for key, value in sorted(counts.items())]
