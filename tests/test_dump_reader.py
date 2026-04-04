"""Tests for dump_reader: multistream index, block parsing, and legacy reader."""

from __future__ import annotations

import bz2
from pathlib import Path
from unittest.mock import patch

import pytest

from wiki_pipeline.dump_reader import (
    _article_dump_name,
    _convert_to_plaintext,
    _decompress_bz2_block,
    _extract_pages_from_block,
    load_multistream_index,
    read_articles_from_dump,
    read_articles_multistream,
    resolve_dump_path,
    resolve_multistream_paths,
    MW_NS,
)


# --- Test fixtures ---


def _make_page_xml(page_id: int, title: str, text: str, ns: int = 0) -> str:
    return (
        f"  <page>\n"
        f"    <title>{title}</title>\n"
        f"    <ns>{ns}</ns>\n"
        f"    <id>{page_id}</id>\n"
        f"    <revision>\n"
        f"      <id>999</id>\n"
        f'      <text bytes="{len(text)}" xml:space="preserve">{text}</text>\n'
        f"    </revision>\n"
        f"  </page>\n"
    )


def _make_multistream_dump(pages_per_block: list[list[tuple[int, str, str]]]) -> tuple[bytes, str]:
    """Build a multistream bz2 dump and index from page specs.

    Args:
        pages_per_block: list of blocks, each block is list of (page_id, title, wikitext)

    Returns:
        (dump_bytes, index_text)
    """
    dump_parts = []
    index_lines = []
    offset = 0

    for i, block_pages in enumerate(pages_per_block):
        xml_parts = []
        if i == 0:
            xml_parts.append(f'<mediawiki xmlns="{MW_NS}">\n')
        for pid, title, text in block_pages:
            xml_parts.append(_make_page_xml(pid, title, text))
            index_lines.append(f"{offset}:{pid}:{title}")
        if i == len(pages_per_block) - 1:
            xml_parts.append("</mediawiki>\n")

        block_xml = "".join(xml_parts).encode("utf-8")
        block_bz2 = bz2.compress(block_xml)
        dump_parts.append(block_bz2)
        offset += len(block_bz2)

    return b"".join(dump_parts), "\n".join(index_lines) + "\n"


@pytest.fixture
def multistream_data(tmp_path):
    """Create a small multistream dump + index in tmp_path."""
    pages = [
        [(100, "Article One", "Wikitext for article one.")],
        [(200, "Article Two", "Wikitext for '''article two'''.")],
        [(300, "Article Three", "Third article [[content]].")],
    ]
    dump_bytes, index_text = _make_multistream_dump(pages)

    dump_path = tmp_path / "test-multistream.xml.bz2"
    dump_path.write_bytes(dump_bytes)

    index_path = tmp_path / "test-multistream-index.txt"
    index_path.write_text(index_text)

    return dump_path, index_path


@pytest.fixture
def multistream_data_bz2_index(tmp_path):
    """Create multistream dump + bz2-compressed index."""
    pages = [
        [(100, "Article One", "Wikitext for article one.")],
        [(200, "Article Two", "Wikitext for article two.")],
    ]
    dump_bytes, index_text = _make_multistream_dump(pages)

    dump_path = tmp_path / "test-multistream.xml.bz2"
    dump_path.write_bytes(dump_bytes)

    index_path = tmp_path / "test-multistream-index.txt.bz2"
    index_path.write_bytes(bz2.compress(index_text.encode("utf-8")))

    return dump_path, index_path


# --- resolve_dump_path ---


class TestResolveDumpPath:
    def test_prefers_uncompressed(self, tmp_path):
        (tmp_path / _article_dump_name("enwiki", "xml")).write_text("x")
        (tmp_path / _article_dump_name("enwiki", "bz2")).write_bytes(b"x")
        result = resolve_dump_path(tmp_path)
        assert result.name == _article_dump_name("enwiki", "xml")

    def test_falls_back_to_bz2(self, tmp_path):
        (tmp_path / _article_dump_name("enwiki", "bz2")).write_bytes(b"x")
        result = resolve_dump_path(tmp_path)
        assert result.name == _article_dump_name("enwiki", "bz2")

    def test_raises_when_missing(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="No article dump"):
            resolve_dump_path(tmp_path)

    def test_custom_wiki(self, tmp_path):
        (tmp_path / _article_dump_name("simplewiki", "bz2")).write_bytes(b"x")
        result = resolve_dump_path(tmp_path, wiki="simplewiki")
        assert "simplewiki" in result.name


# --- resolve_multistream_paths ---


class TestResolveMultistreamPaths:
    def test_returns_both_paths(self, tmp_path):
        (tmp_path / _article_dump_name("enwiki", "multistream")).write_bytes(b"x")
        (tmp_path / _article_dump_name("enwiki", "multistream_index")).write_bytes(b"x")
        dump, index = resolve_multistream_paths(tmp_path)
        assert "multistream.xml.bz2" in dump.name
        assert "multistream-index.txt.bz2" in index.name

    def test_raises_when_dump_missing(self, tmp_path):
        (tmp_path / _article_dump_name("enwiki", "multistream_index")).write_bytes(b"x")
        with pytest.raises(FileNotFoundError, match="Multistream dump not found"):
            resolve_multistream_paths(tmp_path)

    def test_raises_when_index_missing(self, tmp_path):
        (tmp_path / _article_dump_name("enwiki", "multistream")).write_bytes(b"x")
        with pytest.raises(FileNotFoundError, match="Multistream index not found"):
            resolve_multistream_paths(tmp_path)

    def test_custom_wiki(self, tmp_path):
        (tmp_path / _article_dump_name("simplewiki", "multistream")).write_bytes(b"x")
        (tmp_path / _article_dump_name("simplewiki", "multistream_index")).write_bytes(b"x")
        dump, index = resolve_multistream_paths(tmp_path, wiki="simplewiki")
        assert "simplewiki" in dump.name
        assert "simplewiki" in index.name


# --- load_multistream_index ---


class TestLoadMultistreamIndex:
    def test_parses_plain_text_index(self, tmp_path):
        index_text = "0:100:Article One\n0:101:Article Two\n500:200:Article Three\n"
        index_path = tmp_path / "index.txt"
        index_path.write_text(index_text)

        result = load_multistream_index(index_path)
        assert result == {100: 0, 101: 0, 200: 500}

    def test_parses_bz2_index(self, tmp_path):
        index_text = "0:100:Title A\n1024:200:Title B\n"
        index_path = tmp_path / "index.txt.bz2"
        index_path.write_bytes(bz2.compress(index_text.encode("utf-8")))

        result = load_multistream_index(index_path)
        assert result == {100: 0, 200: 1024}

    def test_skips_malformed_lines(self, tmp_path):
        index_text = "0:100:Good Line\nbad line\n::\n0:abc:Bad ID\n500:200:OK\n"
        index_path = tmp_path / "index.txt"
        index_path.write_text(index_text)

        result = load_multistream_index(index_path)
        assert result == {100: 0, 200: 500}

    def test_handles_titles_with_colons(self, tmp_path):
        index_text = "0:100:Category:Art:Painting\n"
        index_path = tmp_path / "index.txt"
        index_path.write_text(index_text)

        result = load_multistream_index(index_path)
        assert result == {100: 0}

    def test_empty_index(self, tmp_path):
        index_path = tmp_path / "index.txt"
        index_path.write_text("")
        result = load_multistream_index(index_path)
        assert result == {}


# --- _decompress_bz2_block ---


class TestDecompressBz2Block:
    def test_decompresses_at_offset_zero(self, tmp_path):
        content = b"Hello, multistream!"
        compressed = bz2.compress(content)
        dump_path = tmp_path / "test.bz2"
        dump_path.write_bytes(compressed)

        with open(dump_path, "rb") as f:
            result = _decompress_bz2_block(f, 0)
        assert result == content

    def test_decompresses_at_nonzero_offset(self, tmp_path):
        block1 = bz2.compress(b"First block")
        block2 = bz2.compress(b"Second block")
        dump_path = tmp_path / "test.bz2"
        dump_path.write_bytes(block1 + block2)

        with open(dump_path, "rb") as f:
            result = _decompress_bz2_block(f, len(block1))
        assert result == b"Second block"

    def test_reads_only_one_stream(self, tmp_path):
        block1 = bz2.compress(b"AAA")
        block2 = bz2.compress(b"BBB")
        dump_path = tmp_path / "test.bz2"
        dump_path.write_bytes(block1 + block2)

        with open(dump_path, "rb") as f:
            result = _decompress_bz2_block(f, 0)
        assert result == b"AAA"


# --- _extract_pages_from_block ---


class TestExtractPagesFromBlock:
    def test_extracts_single_page(self):
        xml = (
            f'<mediawiki xmlns="{MW_NS}">\n'
            + _make_page_xml(100, "Test Page", "Some wikitext")
            + "</mediawiki>"
        )
        result = _extract_pages_from_block(xml.encode("utf-8"))
        assert 100 in result
        assert result[100] == ("Test Page", "Some wikitext")

    def test_extracts_multiple_pages(self):
        xml = (
            f'<mediawiki xmlns="{MW_NS}">\n'
            + _make_page_xml(100, "Page A", "Text A")
            + _make_page_xml(200, "Page B", "Text B")
            + "</mediawiki>"
        )
        result = _extract_pages_from_block(xml.encode("utf-8"))
        assert len(result) == 2
        assert result[100][0] == "Page A"
        assert result[200][0] == "Page B"

    def test_skips_non_article_namespace(self):
        xml = (
            f'<mediawiki xmlns="{MW_NS}">\n'
            + _make_page_xml(100, "Article", "Text", ns=0)
            + _make_page_xml(200, "Category:Art", "Cat text", ns=14)
            + "</mediawiki>"
        )
        result = _extract_pages_from_block(xml.encode("utf-8"))
        assert 100 in result
        assert 200 not in result

    def test_handles_middle_block_no_mediawiki_tags(self):
        xml = (
            _make_page_xml(100, "Middle Page", "Middle text")
        )
        result = _extract_pages_from_block(xml.encode("utf-8"))
        assert 100 in result

    def test_handles_first_block_with_siteinfo(self):
        xml = (
            f'<mediawiki xmlns="{MW_NS}">\n'
            "  <siteinfo><sitename>Wikipedia</sitename></siteinfo>\n"
            + _make_page_xml(100, "First Page", "First text")
        )
        result = _extract_pages_from_block(xml.encode("utf-8"))
        assert 100 in result

    def test_returns_empty_on_parse_error(self):
        result = _extract_pages_from_block(b"<<<not xml>>>")
        assert result == {}

    def test_skips_pages_with_empty_text(self):
        xml = (
            f"  <page>\n"
            f"    <title>Empty</title>\n"
            f"    <ns>0</ns>\n"
            f"    <id>100</id>\n"
            f"    <revision>\n"
            f"      <id>999</id>\n"
            f'      <text bytes="0" xml:space="preserve"></text>\n'
            f"    </revision>\n"
            f"  </page>\n"
        )
        result = _extract_pages_from_block(xml.encode("utf-8"))
        assert 100 not in result


# --- read_articles_multistream ---


class TestReadArticlesMultistream:
    def test_reads_all_articles(self, multistream_data):
        dump_path, index_path = multistream_data
        index = load_multistream_index(index_path)
        wikitext, plaintext = read_articles_multistream(
            dump_path, index, {100, 200, 300}
        )
        assert len(wikitext) == 3
        assert "Article One" in wikitext
        assert "Article Two" in wikitext
        assert "Article Three" in wikitext
        assert len(plaintext) == 3

    def test_reads_subset(self, multistream_data):
        dump_path, index_path = multistream_data
        index = load_multistream_index(index_path)
        wikitext, plaintext = read_articles_multistream(
            dump_path, index, {200}
        )
        assert len(wikitext) == 1
        assert "Article Two" in wikitext

    def test_limit_stops_early(self, multistream_data):
        dump_path, index_path = multistream_data
        index = load_multistream_index(index_path)
        wikitext, plaintext = read_articles_multistream(
            dump_path, index, {100, 200, 300}, limit=1
        )
        assert len(wikitext) == 1

    def test_missing_page_ids_skipped(self, multistream_data):
        dump_path, index_path = multistream_data
        index = load_multistream_index(index_path)
        wikitext, plaintext = read_articles_multistream(
            dump_path, index, {100, 999}
        )
        assert len(wikitext) == 1
        assert "Article One" in wikitext

    def test_empty_page_ids(self, multistream_data):
        dump_path, index_path = multistream_data
        index = load_multistream_index(index_path)
        wikitext, plaintext = read_articles_multistream(
            dump_path, index, set()
        )
        assert wikitext == {}
        assert plaintext == {}

    def test_works_with_bz2_index(self, multistream_data_bz2_index):
        dump_path, index_path = multistream_data_bz2_index
        index = load_multistream_index(index_path)
        wikitext, plaintext = read_articles_multistream(
            dump_path, index, {100, 200}
        )
        assert len(wikitext) == 2

    def test_multiple_pages_per_block(self, tmp_path):
        pages = [
            [
                (100, "Page A", "Text A"),
                (200, "Page B", "Text B"),
            ],
            [
                (300, "Page C", "Text C"),
            ],
        ]
        dump_bytes, index_text = _make_multistream_dump(pages)
        dump_path = tmp_path / "dump.bz2"
        dump_path.write_bytes(dump_bytes)
        index_path = tmp_path / "index.txt"
        index_path.write_text(index_text)

        index = load_multistream_index(index_path)
        wikitext, _ = read_articles_multistream(dump_path, index, {100, 200, 300})
        assert len(wikitext) == 3

    def test_only_reads_targeted_pages_from_block(self, tmp_path):
        pages = [
            [
                (100, "Wanted", "Want this"),
                (200, "Unwanted", "Skip this"),
            ],
        ]
        dump_bytes, index_text = _make_multistream_dump(pages)
        dump_path = tmp_path / "dump.bz2"
        dump_path.write_bytes(dump_bytes)
        index_path = tmp_path / "index.txt"
        index_path.write_text(index_text)

        index = load_multistream_index(index_path)
        wikitext, _ = read_articles_multistream(dump_path, index, {100})
        assert len(wikitext) == 1
        assert "Wanted" in wikitext
        assert "Unwanted" not in wikitext

    def test_plaintext_strips_markup(self, multistream_data):
        dump_path, index_path = multistream_data
        index = load_multistream_index(index_path)
        wikitext, plaintext = read_articles_multistream(
            dump_path, index, {200}
        )
        assert "'''" not in plaintext["Article Two"]
        assert "article two" in plaintext["Article Two"]


# --- _convert_to_plaintext ---


class TestConvertToPlaintext:
    def test_strips_bold(self):
        assert "hello" in _convert_to_plaintext("'''hello'''")

    def test_strips_links(self):
        result = _convert_to_plaintext("[[Paris|city of Paris]]")
        assert "city of Paris" in result

    def test_handles_plain_text(self):
        assert _convert_to_plaintext("just text") == "just text"


# --- Legacy reader ---


class TestReadArticlesFromDump:
    def test_reads_uncompressed_xml(self, tmp_path):
        ns = MW_NS
        xml = (
            f'<mediawiki xmlns="{ns}">\n'
            f"  <page>\n"
            f"    <title>Test Article</title>\n"
            f"    <ns>0</ns>\n"
            f"    <id>100</id>\n"
            f"    <revision>\n"
            f"      <id>1</id>\n"
            f'      <text bytes="10" xml:space="preserve">Hello world</text>\n'
            f"    </revision>\n"
            f"  </page>\n"
            f"  <page>\n"
            f"    <title>Category:Art</title>\n"
            f"    <ns>14</ns>\n"
            f"    <id>200</id>\n"
            f"    <revision>\n"
            f"      <id>2</id>\n"
            f'      <text bytes="5" xml:space="preserve">cat</text>\n'
            f"    </revision>\n"
            f"  </page>\n"
            f"</mediawiki>\n"
        )
        dump_path = tmp_path / "dump.xml"
        dump_path.write_text(xml)

        wikitext, plaintext = read_articles_from_dump(dump_path, {100, 200})
        assert len(wikitext) == 1
        assert "Test Article" in wikitext
        assert wikitext["Test Article"] == "Hello world"

    def test_limit(self, tmp_path):
        ns = MW_NS
        xml = (
            f'<mediawiki xmlns="{ns}">\n'
            + _make_page_xml(100, "A", "Text A")
            + _make_page_xml(200, "B", "Text B")
            + "</mediawiki>\n"
        )
        # Need to add namespace to page elements for iterparse
        xml = xml.replace("<page>", f"<page>")
        dump_path = tmp_path / "dump.xml"
        dump_path.write_text(xml)

        wikitext, _ = read_articles_from_dump(dump_path, {100, 200}, limit=1)
        assert len(wikitext) == 1
