"""Simple local watermark helpers for incremental extraction scaffolding."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

from src.common.utils import utc_now_iso


def read_watermark_state(path: str | Path) -> dict[str, str | None]:
    """Read a local watermark state file or return an empty state."""
    state_path = Path(path)
    if not state_path.exists():
        return {"column_name": None, "value": None, "updated_at": None}

    payload = json.loads(state_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Watermark state must be stored as a JSON object.")

    return {
        "column_name": payload.get("column_name"),
        "value": payload.get("value"),
        "updated_at": payload.get("updated_at"),
    }


def get_latest_watermark(values: Iterable[str | None]) -> str | None:
    """Return the latest non-null watermark value from an iterable."""
    candidates = [value for value in values if value]
    return max(candidates) if candidates else None


def build_watermark_state(column_name: str, value: str | None) -> dict[str, str | None]:
    """Create a small metadata structure for checkpoint persistence."""
    return {
        "column_name": column_name,
        "value": value,
        "updated_at": utc_now_iso(),
    }


def write_watermark_state(
    path: str | Path,
    column_name: str,
    value: str | None,
) -> dict[str, str | None]:
    """Write a local JSON watermark state file and return the stored state."""
    state = build_watermark_state(column_name=column_name, value=value)
    state_path = Path(path)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(f"{json.dumps(state, indent=2)}\n", encoding="utf-8")
    return state
