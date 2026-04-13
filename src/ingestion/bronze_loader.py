"""Helpers for shaping raw records into bronze-ready rows."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Iterable
from uuid import uuid4

from src.common.constants import SOURCE_SYSTEM


def _payload_hash(payload: dict[str, Any]) -> str:
    """Create a stable hash from a raw JSON payload."""
    raw = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def build_bronze_record(
    raw_record: dict[str, Any],
    source_system: str = SOURCE_SYSTEM,
    file_path: str = "<pending-file-path>",
    api_pull_timestamp: str | None = None,
    ingest_timestamp: datetime | None = None,
) -> dict[str, Any]:
    """Attach ingestion metadata to a single source record."""
    ingest_ts = ingest_timestamp or datetime.now(timezone.utc)
    return {
        "ingest_id": str(uuid4()),
        "source_system": source_system,
        "source_record_id": str(raw_record.get("unique_key", "<missing-source-record-id>")),
        "ingest_timestamp": ingest_ts.isoformat(),
        "ingest_date": ingest_ts.date().isoformat(),
        "api_pull_timestamp": api_pull_timestamp or ingest_ts.isoformat(),
        "raw_payload": json.dumps(raw_record, sort_keys=True, default=str),
        "record_hash": _payload_hash(raw_record),
        "file_path": file_path,
    }


def build_bronze_file_path(
    bronze_base_path: str,
    ingest_timestamp: datetime,
    batch_id: str,
) -> str:
    """Build a partition-style landing path for a bronze batch."""
    base_path = bronze_base_path.rstrip("/")
    return f"{base_path}/ingest_date={ingest_timestamp.date().isoformat()}/batch_{batch_id}.json"


def prepare_bronze_batch(
    records: Iterable[dict[str, Any]],
    bronze_base_path: str = "<pending-bronze-path>",
    source_system: str = SOURCE_SYSTEM,
    api_pull_timestamp: str | None = None,
    ingest_timestamp: datetime | None = None,
    batch_id: str | None = None,
) -> list[dict[str, Any]]:
    """Prepare a bronze batch with shared load metadata."""
    resolved_ingest_timestamp = ingest_timestamp or datetime.now(timezone.utc)
    resolved_batch_id = str(batch_id).strip() if batch_id else str(uuid4())
    file_path = build_bronze_file_path(bronze_base_path, resolved_ingest_timestamp, resolved_batch_id)

    return [
        build_bronze_record(
            raw_record=record,
            source_system=source_system,
            file_path=file_path,
            api_pull_timestamp=api_pull_timestamp,
            ingest_timestamp=resolved_ingest_timestamp,
        )
        for record in records
    ]
