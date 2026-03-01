"""Filter page IDs by namespace, length, and redirect status."""

from __future__ import annotations

from dataclasses import dataclass

from .sql_parser import iter_rows


@dataclass
class PageInfo:
    page_id: int
    title: str
    length: int


def filter_pages(
    page_path: str | object,
    candidate_ids: set[int],
    min_length: int,
) -> list[PageInfo]:
    """Single streaming pass through page.sql.gz — keep ns=0, non-redirect, length >= threshold."""
    results: list[PageInfo] = []
    for row in iter_rows(page_path, "page"):
        # 0=page_id, 1=namespace, 2=title, 3=is_redirect, 9=page_len
        page_id = int(row[0])
        if page_id not in candidate_ids:
            continue
        if row[1] != "0":
            continue
        if row[3] == "1":
            continue
        page_len = int(row[9])
        if page_len < min_length:
            continue
        results.append(PageInfo(page_id=page_id, title=row[2], length=page_len))
    return results


def filter_pages_from_meta(
    page_meta: dict[int, tuple[str, int]],
    candidate_ids: set[int],
    min_length: int,
) -> list[PageInfo]:
    """Filter from cached page_meta dict instead of re-parsing dump.

    page_meta already excludes redirects and non-ns=0 pages.
    """
    results: list[PageInfo] = []
    for page_id in candidate_ids:
        entry = page_meta.get(page_id)
        if entry is None:
            continue
        title, length = entry
        if length < min_length:
            continue
        results.append(PageInfo(page_id=page_id, title=title, length=length))
    return results
