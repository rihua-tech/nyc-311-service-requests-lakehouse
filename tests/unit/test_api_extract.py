from urllib.parse import parse_qs, urlparse

import pytest

from src.ingestion.api_extract import (
    build_query_params,
    build_request_url,
    extract_service_requests,
    fetch_page,
)


def test_build_query_params_includes_pagination_and_watermark() -> None:
    params = build_query_params(
        limit=1000,
        offset=2000,
        watermark_field="created_date",
        watermark_value="2024-01-01T00:00:00",
        order_by="created_date ASC",
    )

    assert params["$limit"] == "1000"
    assert params["$offset"] == "2000"
    assert params["$order"] == "created_date ASC"
    assert params["$where"] == "created_date >= '2024-01-01T00:00:00'"


def test_build_request_url_renders_query_string() -> None:
    url = build_request_url("https://example.org/data.json", {"$limit": "2", "$offset": "4"})

    assert url.startswith("https://example.org/data.json?")
    assert "%24limit=2" in url
    assert "%24offset=4" in url


def test_fetch_page_validates_fetcher_output() -> None:
    with pytest.raises(ValueError):
        fetch_page(
            "https://example.org/data.json",
            {"$limit": "1"},
            page_fetcher=lambda _url, _timeout: {"unexpected": "shape"},  # type: ignore[return-value]
        )


def test_extract_service_requests_fetches_multiple_pages() -> None:
    sample_rows = [
        {"unique_key": "1001"},
        {"unique_key": "1002"},
        {"unique_key": "1003"},
    ]

    def fake_page_fetcher(url: str, _timeout: int) -> list[dict[str, str]]:
        params = parse_qs(urlparse(url).query)
        limit = int(params["$limit"][0])
        offset = int(params["$offset"][0])
        return sample_rows[offset : offset + limit]

    rows = extract_service_requests(
        base_url="https://example.org/data.json",
        page_size=2,
        watermark_field="created_date",
        watermark_value="2024-01-01T00:00:00",
        order_by="created_date ASC",
        page_fetcher=fake_page_fetcher,
    )

    assert [row["unique_key"] for row in rows] == ["1001", "1002", "1003"]

