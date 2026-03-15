"""Starter builders for gold fact tables."""

from __future__ import annotations

from typing import Any, Iterable


def build_fact_service_requests(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Create a minimal fact table projection from silver rows."""
    fact_rows: list[dict[str, Any]] = []
    for record in records:
        fact_rows.append(
            {
                "request_id": record.get("request_id"),
                "created_date": record.get("created_date"),
                "closed_date": record.get("closed_date"),
                "agency_code": record.get("agency_code"),
                "complaint_type": record.get("complaint_type"),
                "status_name": record.get("status_name"),
                "incident_zip": record.get("incident_zip"),
                "resolution_time_hours": record.get("resolution_time_hours"),
                "is_closed": record.get("is_closed"),
                "is_overdue": record.get("is_overdue"),
            }
        )
    return fact_rows

