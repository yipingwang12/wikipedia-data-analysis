"""Tests for page_filter — namespace, length, redirect filtering."""

from __future__ import annotations

import io

from wiki_pipeline.page_filter import PageInfo, filter_pages, filter_pages_from_meta


def _page_sql(*rows: tuple) -> io.StringIO:
    """Build page.sql INSERT from (id, ns, title, is_redirect, page_len) tuples.

    Actual schema: page_id(0), page_namespace(1), page_title(2), page_is_redirect(3),
    page_is_new(4), page_random(5), page_touched(6), page_links_updated(7),
    page_latest(8), page_len(9), page_content_model(10), page_lang(11)
    """
    vals = []
    for pid, ns, title, redir, plen in rows:
        vals.append(
            f"({pid},{ns},'{title}',{redir},0,0.1,'20240101',NULL,1,{plen},'wikitext',NULL)"
        )
    return io.StringIO(f"INSERT INTO `page` VALUES {','.join(vals)};\n")


class TestFilterPages:
    def test_filters_by_candidate_ids(self):
        sql = _page_sql((1, 0, "A", 0, 6000), (2, 0, "B", 0, 6000))
        result = filter_pages(sql, {1}, 5000)
        assert len(result) == 1
        assert result[0].page_id == 1

    def test_filters_namespace(self):
        sql = _page_sql((1, 0, "Article", 0, 6000), (2, 14, "Category", 0, 6000))
        result = filter_pages(sql, {1, 2}, 5000)
        assert len(result) == 1
        assert result[0].title == "Article"

    def test_filters_length(self):
        sql = _page_sql((1, 0, "Short", 0, 100), (2, 0, "Long", 0, 10000))
        result = filter_pages(sql, {1, 2}, 5000)
        assert len(result) == 1
        assert result[0].title == "Long"

    def test_filters_redirects(self):
        sql = _page_sql((1, 0, "Real", 0, 6000), (2, 0, "Redir", 1, 6000))
        result = filter_pages(sql, {1, 2}, 5000)
        assert len(result) == 1
        assert result[0].title == "Real"

    def test_empty_candidate_set(self):
        sql = _page_sql((1, 0, "A", 0, 6000))
        result = filter_pages(sql, set(), 5000)
        assert result == []

    def test_returns_page_info(self):
        sql = _page_sql((42, 0, "Test_Article", 0, 8000))
        result = filter_pages(sql, {42}, 5000)
        assert len(result) == 1
        assert result[0] == PageInfo(page_id=42, title="Test_Article", length=8000)


class TestFilterPagesFromMeta:
    def test_basic_filtering(self):
        meta = {1: ("A", 6000), 2: ("B", 3000), 3: ("C", 8000)}
        result = filter_pages_from_meta(meta, {1, 2, 3}, 5000)
        titles = {p.title for p in result}
        assert titles == {"A", "C"}

    def test_candidate_id_filtering(self):
        meta = {1: ("A", 6000), 2: ("B", 6000)}
        result = filter_pages_from_meta(meta, {1}, 5000)
        assert len(result) == 1
        assert result[0].page_id == 1

    def test_empty_candidates(self):
        meta = {1: ("A", 6000)}
        assert filter_pages_from_meta(meta, set(), 5000) == []

    def test_no_matches(self):
        meta = {1: ("A", 100)}
        assert filter_pages_from_meta(meta, {1}, 5000) == []

    def test_returns_page_info(self):
        meta = {42: ("Test_Article", 8000)}
        result = filter_pages_from_meta(meta, {42}, 5000)
        assert result[0] == PageInfo(page_id=42, title="Test_Article", length=8000)
