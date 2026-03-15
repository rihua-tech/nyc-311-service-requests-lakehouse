"""Shared helper functions for lightweight local logic."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def utc_now_iso() -> str:
    """Return the current UTC timestamp in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat()


def parse_optional_float(value: Any) -> float | None:
    """Best-effort float parsing for optional numeric fields."""
    if value in (None, ""):
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_optional_datetime(value: Any) -> datetime | None:
    """Best-effort ISO datetime parsing used by starter transforms."""
    if value in (None, ""):
        return None

    text = str(value).replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def build_storage_path(container: str, *parts: str) -> str:
    """Build a simple slash-delimited storage path."""
    clean_parts = [container.strip("/")] + [part.strip("/") for part in parts if part]
    return "/".join(clean_parts)

