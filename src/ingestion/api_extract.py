"""Minimal helpers for extracting paginated NYC 311 API records."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen

from src.common.logger import get_logger

logger = get_logger(__name__)

JsonRecord = dict[str, Any]
PageFetcher = Callable[[str, int], list[JsonRecord]]


def build_query_params(
    limit: int,
    offset: int = 0,
    watermark_field: str = "created_date",
    watermark_value: str | None = None,
    order_by: str | None = None,
) -> dict[str, str]:
    """Build a minimal Socrata-style query parameter dictionary."""
    if limit <= 0:
        raise ValueError("limit must be greater than zero")

    params = {"$limit": str(limit), "$offset": str(offset)}
    if order_by:
        params["$order"] = order_by
    if watermark_value:
        # TODO: Revisit filter semantics once the production watermark strategy is locked in.
        params["$where"] = f"{watermark_field} >= '{watermark_value}'"
    return params


def build_request_url(base_url: str, params: Mapping[str, str]) -> str:
    """Compose the request URL for a single API page."""
    return f"{base_url}?{urlencode(params)}"


def _default_page_fetcher(url: str, timeout: int) -> list[JsonRecord]:
    """Fetch a single page from the API using the standard library."""
    with urlopen(url, timeout=timeout) as response:
        payload = json.loads(response.read().decode("utf-8"))

    if not isinstance(payload, list):
        raise ValueError("Expected the API to return a list of records.")

    return payload


def fetch_page(
    base_url: str,
    params: Mapping[str, str],
    timeout: int = 30,
    page_fetcher: PageFetcher | None = None,
) -> list[JsonRecord]:
    """Fetch one page of records using a configurable fetcher."""
    request_url = build_request_url(base_url, params)
    fetcher = page_fetcher or _default_page_fetcher
    records = fetcher(request_url, timeout)

    if not isinstance(records, list):
        raise ValueError("Page fetcher must return a list of records.")

    logger.info("Fetched %s records from %s", len(records), request_url)
    return records


def extract_service_requests(
    base_url: str,
    page_size: int = 1000,
    watermark_field: str = "created_date",
    watermark_value: str | None = None,
    max_pages: int | None = None,
    timeout: int = 30,
    order_by: str | None = None,
    page_fetcher: PageFetcher | None = None,
) -> list[JsonRecord]:
    """Fetch paginated records from the configured endpoint."""
    if max_pages is not None and max_pages <= 0:
        raise ValueError("max_pages must be greater than zero when provided")

    records: list[JsonRecord] = []
    pages_fetched = 0
    offset = 0

    while True:
        params = build_query_params(
            limit=page_size,
            offset=offset,
            watermark_field=watermark_field,
            watermark_value=watermark_value,
            order_by=order_by,
        )
        page = fetch_page(base_url, params=params, timeout=timeout, page_fetcher=page_fetcher)

        if not page:
            break

        records.extend(page)
        pages_fetched += 1

        if len(page) < page_size:
            break

        if max_pages is not None and pages_fetched >= max_pages:
            break

        offset += page_size

    logger.info("Extracted %s records across %s page(s)", len(records), pages_fetched)
    return records
