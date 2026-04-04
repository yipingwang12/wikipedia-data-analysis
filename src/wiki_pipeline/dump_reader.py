"""Stream article wikitext from XML dump (uncompressed, bz2, or multistream bz2)."""

from __future__ import annotations

import bz2
import re
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import mwparserfromhell

MW_NS = "http://www.mediawiki.org/xml/export-0.10/"

def _article_dump_name(wiki: str, variant: str) -> str:
    """Generate article dump filename. variant: xml, bz2, multistream, multistream_index."""
    suffixes = {
        "xml": "pages-articles.xml",
        "bz2": "pages-articles.xml.bz2",
        "multistream": "pages-articles-multistream.xml.bz2",
        "multistream_index": "pages-articles-multistream-index.txt.bz2",
    }
    return f"{wiki}-latest-{suffixes[variant]}"

_MEDIAWIKI_TAG_RE = re.compile(r"</?mediawiki[^>]*>")
_SITEINFO_RE = re.compile(r"<siteinfo>.*?</siteinfo>", re.DOTALL)


def _open_dump(dump_path: Path):
    """Open dump file, auto-detecting compression from extension."""
    if dump_path.suffix == ".bz2":
        return bz2.open(dump_path, "rb")
    return open(dump_path, "rb")


def resolve_dump_path(data_dir: Path, wiki: str = "enwiki") -> Path:
    """Return path to best available legacy dump: prefer uncompressed, fall back to bz2."""
    uncompressed = data_dir / _article_dump_name(wiki, "xml")
    if uncompressed.exists():
        return uncompressed
    compressed = data_dir / _article_dump_name(wiki, "bz2")
    if compressed.exists():
        return compressed
    raise FileNotFoundError(
        f"No article dump found in {data_dir}\n"
        "Run with --download-articles first, or use --use-api to fetch via API."
    )


def resolve_multistream_paths(
    data_dir: Path, wiki: str = "enwiki"
) -> tuple[Path, Path]:
    """Return (dump_path, index_path) for multistream files."""
    dump = data_dir / _article_dump_name(wiki, "multistream")
    index = data_dir / _article_dump_name(wiki, "multistream_index")
    if not dump.exists():
        raise FileNotFoundError(
            f"Multistream dump not found: {dump}\n"
            "Run with --download-articles first."
        )
    if not index.exists():
        raise FileNotFoundError(
            f"Multistream index not found: {index}\n"
            "Run with --download-articles first."
        )
    return dump, index


# --- Multistream index ---


def load_multistream_index(index_path: Path) -> dict[int, int]:
    """Parse multistream index file, return {page_id: byte_offset}.

    Index format: one line per page, "byte_offset:page_id:title".
    """
    index: dict[int, int] = {}
    opener = bz2.open if index_path.suffix == ".bz2" else open
    t0 = time.time()
    with opener(index_path, "rt", encoding="utf-8") as f:
        for line in f:
            parts = line.split(":", 2)
            if len(parts) < 3:
                continue
            try:
                offset = int(parts[0])
                page_id = int(parts[1])
                index[page_id] = offset
            except ValueError:
                continue
    elapsed = time.time() - t0
    sys.stderr.write(
        f"  Loaded multistream index: {len(index)} pages [{elapsed:.1f}s]\n"
    )
    sys.stderr.flush()
    return index


# --- Multistream reader ---


def _decompress_bz2_block(f, offset: int) -> bytes:
    """Decompress one bz2 stream starting at byte offset."""
    f.seek(offset)
    decompressor = bz2.BZ2Decompressor()
    chunks = []
    while not decompressor.eof:
        data = f.read(65536)
        if not data:
            break
        chunks.append(decompressor.decompress(data))
    return b"".join(chunks)


def _extract_pages_from_block(data: bytes) -> dict[int, tuple[str, str]]:
    """Parse decompressed XML block, return {page_id: (title, wikitext)}.

    Handles first-block mediawiki/siteinfo headers and last-block close tags.
    """
    text = data.decode("utf-8", errors="replace")
    text = _MEDIAWIKI_TAG_RE.sub("", text)
    text = _SITEINFO_RE.sub("", text)
    wrapped = f'<root xmlns="{MW_NS}">{text}</root>'.encode("utf-8")

    pages: dict[int, tuple[str, str]] = {}
    try:
        root = ET.fromstring(wrapped)
    except ET.ParseError:
        return pages

    ns = f"{{{MW_NS}}}"
    for page in root.findall(f"{ns}page"):
        ns_elem = page.find(f"{ns}ns")
        if ns_elem is None or ns_elem.text != "0":
            continue
        id_elem = page.find(f"{ns}id")
        title_elem = page.find(f"{ns}title")
        text_elem = page.find(f"{ns}revision/{ns}text")
        if (
            id_elem is not None
            and title_elem is not None
            and text_elem is not None
            and text_elem.text
        ):
            pages[int(id_elem.text)] = (title_elem.text, text_elem.text)
    return pages


def _convert_to_plaintext(wikitext: str) -> str:
    """Convert wikitext to plaintext via mwparserfromhell."""
    try:
        return mwparserfromhell.parse(wikitext).strip_code()
    except Exception:
        return wikitext


def read_articles_multistream(
    dump_path: Path,
    index: dict[int, int],
    page_ids: set[int],
    limit: int | None = None,
) -> tuple[dict[str, str], dict[str, str]]:
    """Read articles via multistream index (fast random access).

    Returns (wikitext_map, plaintext_map) keyed by title.
    Only decompresses bz2 blocks containing target pages.
    """
    # Map target page_ids to block offsets
    offset_to_targets: dict[int, set[int]] = {}
    missing = 0
    for pid in page_ids:
        offset = index.get(pid)
        if offset is None:
            missing += 1
            continue
        offset_to_targets.setdefault(offset, set()).add(pid)

    if missing:
        sys.stderr.write(
            f"  {missing} page IDs not in multistream index (skipped)\n"
        )
        sys.stderr.flush()

    # Sort offsets for sequential I/O
    sorted_offsets = sorted(offset_to_targets)
    target_count = len(page_ids) - missing
    if limit:
        target_count = min(target_count, limit)

    wikitext_map: dict[str, str] = {}
    plaintext_map: dict[str, str] = {}
    found = 0
    start = time.time()

    with open(dump_path, "rb") as f:
        for offset in sorted_offsets:
            block_data = _decompress_bz2_block(f, offset)
            block_pages = _extract_pages_from_block(block_data)

            for pid, (title, wikitext) in block_pages.items():
                if pid not in offset_to_targets[offset]:
                    continue
                wikitext_map[title] = wikitext
                plaintext_map[title] = _convert_to_plaintext(wikitext)

                found += 1
                elapsed = time.time() - start
                rate = found / max(elapsed, 0.001)
                sys.stderr.write(
                    f"\r  Multistream read: {found}/{target_count} articles  "
                    f"[{elapsed:.0f}s, {rate:.0f}/s]"
                )
                sys.stderr.flush()

                if limit and found >= limit:
                    break

            if limit and found >= limit:
                break

    sys.stderr.write("\n")
    sys.stderr.flush()
    return wikitext_map, plaintext_map


# --- Legacy full-scan reader ---


def read_articles_from_dump(
    dump_path: Path,
    page_ids: set[int],
    limit: int | None = None,
) -> tuple[dict[str, str], dict[str, str]]:
    """Stream full XML dump, return (wikitext_map, plaintext_map) for matching page IDs.

    Keys are article titles (with spaces, matching API convention).
    Scans the entire dump sequentially — use read_articles_multistream for fast access.
    """
    wikitext_map: dict[str, str] = {}
    plaintext_map: dict[str, str] = {}
    target = min(len(page_ids), limit) if limit else len(page_ids)
    found = 0
    start = time.time()

    with _open_dump(dump_path) as f:
        for event, elem in ET.iterparse(f, events=("end",)):
            if not elem.tag.endswith("}page"):
                continue

            ns_elem = elem.find(
                "{http://www.mediawiki.org/xml/export-0.10/}ns"
            )
            if ns_elem is None or ns_elem.text != "0":
                elem.clear()
                continue

            id_elem = elem.find(
                "{http://www.mediawiki.org/xml/export-0.10/}id"
            )
            if id_elem is None:
                elem.clear()
                continue

            page_id = int(id_elem.text)
            if page_id not in page_ids:
                elem.clear()
                continue

            title_elem = elem.find(
                "{http://www.mediawiki.org/xml/export-0.10/}title"
            )
            rev_elem = elem.find(
                "{http://www.mediawiki.org/xml/export-0.10/}revision/"
                "{http://www.mediawiki.org/xml/export-0.10/}text"
            )

            if (
                title_elem is not None
                and rev_elem is not None
                and rev_elem.text
            ):
                title = title_elem.text
                wikitext = rev_elem.text
                wikitext_map[title] = wikitext
                plaintext_map[title] = _convert_to_plaintext(wikitext)

                found += 1
                elapsed = time.time() - start
                sys.stderr.write(
                    f"\r  Reading dump: {found}/{target} articles found  "
                    f"[{elapsed:.0f}s]"
                )
                sys.stderr.flush()

                if found >= target:
                    break

            elem.clear()

    sys.stderr.write("\n")
    sys.stderr.flush()
    return wikitext_map, plaintext_map
