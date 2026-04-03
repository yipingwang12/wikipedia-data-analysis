"""Stream article wikitext from XML dump (uncompressed or bz2)."""

from __future__ import annotations

import bz2
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import mwparserfromhell

UNCOMPRESSED_DUMP = "enwiki-latest-pages-articles.xml"
COMPRESSED_DUMP = "enwiki-latest-pages-articles.xml.bz2"


def _open_dump(dump_path: Path):
    """Open dump file, auto-detecting compression from extension."""
    if dump_path.suffix == ".bz2":
        return bz2.open(dump_path, "rb")
    return open(dump_path, "rb")


def resolve_dump_path(data_dir: Path) -> Path:
    """Return path to best available dump: prefer uncompressed, fall back to bz2."""
    uncompressed = data_dir / UNCOMPRESSED_DUMP
    if uncompressed.exists():
        return uncompressed
    compressed = data_dir / COMPRESSED_DUMP
    if compressed.exists():
        return compressed
    raise FileNotFoundError(
        f"No article dump found in {data_dir}\n"
        "Run with --download-articles first, or use --use-api to fetch via API."
    )


def read_articles_from_dump(
    dump_path: Path,
    page_ids: set[int],
    limit: int | None = None,
) -> tuple[dict[str, str], dict[str, str]]:
    """Stream XML dump, return (wikitext_map, plaintext_map) for matching page IDs.

    Keys are article titles (with spaces, matching API convention).
    If limit is set, stops after that many articles are found.
    Handles both uncompressed .xml and compressed .xml.bz2.
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

            ns_elem = elem.find("{http://www.mediawiki.org/xml/export-0.10/}ns")
            if ns_elem is None or ns_elem.text != "0":
                elem.clear()
                continue

            id_elem = elem.find("{http://www.mediawiki.org/xml/export-0.10/}id")
            if id_elem is None:
                elem.clear()
                continue

            page_id = int(id_elem.text)
            if page_id not in page_ids:
                elem.clear()
                continue

            title_elem = elem.find("{http://www.mediawiki.org/xml/export-0.10/}title")
            rev_elem = elem.find(
                "{http://www.mediawiki.org/xml/export-0.10/}revision/"
                "{http://www.mediawiki.org/xml/export-0.10/}text"
            )

            if title_elem is not None and rev_elem is not None and rev_elem.text:
                title = title_elem.text
                wikitext = rev_elem.text
                wikitext_map[title] = wikitext
                try:
                    parsed = mwparserfromhell.parse(wikitext)
                    plaintext_map[title] = parsed.strip_code()
                except Exception:
                    plaintext_map[title] = wikitext

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
