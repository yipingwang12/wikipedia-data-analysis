"""Extract bio data for all biographical pattern sets from simplewiki via multistream."""

from __future__ import annotations

import bz2
import pickle
import re
import time
from pathlib import Path

import openpyxl

from wiki_pipeline.category_tree import (
    ParsedCategoryLinks,
    collect_articles_from_categories,
    find_categories_by_regex,
)
from wiki_pipeline.dump_reader import load_multistream_index, read_articles_multistream
from wiki_pipeline.infobox_parser import extract_infobox_fields
from wiki_pipeline.nlp_extractor import extract_from_text
from wiki_pipeline.page_filter import filter_pages_from_meta

DATA_DIR = Path("data")
CACHE_DIR = DATA_DIR / ".cache"
PATTERNS_DIR = Path("patterns")
RESULTS_DIR = Path("results/test_simplewiki_all_bio")
REQUIRED_FIELDS = ("birth_date", "death_date", "nationality", "occupation")
MIN_PAGE_LENGTH = 5000

SIMPLEWIKI_DUMP = DATA_DIR / "simplewiki-latest-pages-articles-multistream.xml.bz2"
SIMPLEWIKI_INDEX = DATA_DIR / "simplewiki-latest-pages-articles-multistream-index.txt.bz2"

# --- Date normalization ---

MONTHS = {
    "january": "01", "february": "02", "march": "03", "april": "04",
    "may": "05", "june": "06", "july": "07", "august": "08",
    "september": "09", "october": "10", "november": "11", "december": "12",
    "jan": "01", "feb": "02", "mar": "03", "apr": "04",
    "jun": "06", "jul": "07", "aug": "08", "sep": "09",
    "oct": "10", "nov": "11", "dec": "12",
}

_AGED_RE = re.compile(r"\s*[,(]?\s*aged\s+(?:about\s+)?\d+[^)]*\)?\s*,?\s*$", re.I)
_AGE_RE = re.compile(r"\s*\(age\s+\d+[^)]*\)\s*$", re.I)
_HTML_RE = re.compile(r"&\w+;")
_ORDINAL_RE = re.compile(r"(\d+)(?:st|nd|rd|th)\b", re.I)
_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_YEAR_ONLY = re.compile(r"^\d{3,4}$")
_DD_MONTH_YYYY = re.compile(r"^(\d{1,2})\s+(\w+)\s+(\d{3,4})$")
_MONTH_DD_YYYY = re.compile(r"^(\w+)\s+(\d{1,2}),?\s+(\d{3,4})$")
_MONTH_YYYY = re.compile(r"^(\w+)\s+(\d{3,4})$")


def normalize_date(raw: str | None) -> str | None:
    if not raw:
        return None
    s = raw.strip()
    s = s.replace("&ndash;", "-").replace("&nbsp;", " ")
    s = _HTML_RE.sub("", s)
    range_m = re.match(r"^(\d{3,4})[-/]\d{2,4}$", s)
    if range_m:
        return range_m.group(1)
    s = _AGED_RE.sub("", s)
    s = _AGE_RE.sub("", s)
    s = _ORDINAL_RE.sub(r"\1", s)
    s = re.sub(r"^[Cc]\.\s*", "", s)
    s = re.sub(r"^(\d{1,2}\s+\w+)\s+\1\s+(\d{3,4})$", r"\1 \2", s)
    s = s.strip().rstrip(",")
    s = re.sub(r"^(\d{1,2}\s+\w+),\s+(\d{3,4})$", r"\1 \2", s)

    if _ISO_DATE.match(s):
        return s
    if _YEAR_ONLY.match(s):
        return s

    m = _DD_MONTH_YYYY.match(s)
    if m:
        day, month_name, year = m.groups()
        mm = MONTHS.get(month_name.lower())
        if mm:
            return f"{year}-{mm}-{int(day):02d}"

    m = _MONTH_DD_YYYY.match(s)
    if m:
        month_name, day, year = m.groups()
        mm = MONTHS.get(month_name.lower())
        if mm:
            return f"{year}-{mm}-{int(day):02d}"

    m = _MONTH_YYYY.match(s)
    if m:
        month_name, year = m.groups()
        mm = MONTHS.get(month_name.lower())
        if mm:
            return f"{year}-{mm}"

    return s


# --- Main ---


def load_cache(name: str) -> object:
    with open(CACHE_DIR / f"{name}.pkl", "rb") as f:
        return pickle.load(f)  # noqa: S301


def main() -> None:
    t_start = time.monotonic()

    # 1. Load enwiki caches
    print("Loading enwiki caches...", flush=True)
    t0 = time.monotonic()
    catid_map: dict[int, str] = load_cache("catid_map")
    page_meta: dict[int, tuple[str, int]] = load_cache("page_meta")
    parsed_catlinks: ParsedCategoryLinks = load_cache("parsed_catlinks")
    print(f"  Loaded [{time.monotonic() - t0:.1f}s]", flush=True)

    # 2. Load all bio pattern files, search per file and collect titles
    all_cat_names = set(catid_map.values())
    pattern_files = sorted(PATTERNS_DIR.glob("*.txt"))
    print(f"\nSearching {len(pattern_files)} pattern files...", flush=True)

    per_file_titles: dict[str, set[str]] = {}  # stem → titles (with overlap)
    per_file_stats: list[tuple[str, int, int, int]] = []
    all_titles: set[str] = set()

    t0 = time.monotonic()
    for pf in pattern_files:
        patterns = [l.strip() for l in pf.read_text().splitlines() if l.strip()]
        matched_cats = find_categories_by_regex(all_cat_names, patterns)
        article_ids = collect_articles_from_categories(parsed_catlinks, matched_cats)
        pages = filter_pages_from_meta(page_meta, article_ids, MIN_PAGE_LENGTH)
        titles = {p.title.replace("_", " ") for p in pages}

        per_file_titles[pf.stem] = titles
        all_titles |= titles

        per_file_stats.append((pf.stem, len(patterns), len(matched_cats), len(pages)))
        print(f"  {pf.stem:30s}  {len(patterns):3d} patterns  "
              f"{len(matched_cats):6d} cats  {len(pages):7d} articles", flush=True)

    t_search = time.monotonic() - t0
    print(f"\n  Total: {len(all_titles)} unique titles (deduped) [{t_search:.1f}s]", flush=True)

    # 3. Load simplewiki index, build title → page_id map
    print("\nLoading simplewiki multistream index...", flush=True)
    t0 = time.monotonic()
    sw_index = load_multistream_index(SIMPLEWIKI_INDEX)
    sw_title_to_pid: dict[str, int] = {}
    with bz2.open(SIMPLEWIKI_INDEX, "rt", encoding="utf-8") as f:
        for line in f:
            parts = line.split(":", 2)
            if len(parts) < 3:
                continue
            try:
                pid = int(parts[1])
                title = parts[2].strip()
                sw_title_to_pid[title] = pid
            except ValueError:
                continue
    print(f"  {len(sw_title_to_pid)} simplewiki titles [{time.monotonic() - t0:.1f}s]", flush=True)

    # 4. Find title intersection
    sw_title_set = set(sw_title_to_pid.keys())
    matched_titles = all_titles & sw_title_set
    matched_pids = {sw_title_to_pid[t] for t in matched_titles}
    print(f"  {len(matched_titles)}/{len(all_titles)} titles found in simplewiki "
          f"({len(matched_titles)/len(all_titles)*100:.1f}%)", flush=True)

    if not matched_titles:
        print("No matching articles found.")
        return

    # 5. Read articles from simplewiki multistream dump
    print(f"\nReading {len(matched_pids)} articles from simplewiki dump...", flush=True)
    t0 = time.monotonic()
    wikitext_map, plaintext_map = read_articles_multistream(
        SIMPLEWIKI_DUMP, sw_index, matched_pids
    )
    t_read = time.monotonic() - t0
    print(f"  Read {len(wikitext_map)} articles [{t_read:.1f}s]", flush=True)

    # 6. Extract bio fields: infobox → NLP (no LLM)
    print("Extracting biographical fields...", flush=True)
    t0 = time.monotonic()
    all_records: dict[str, dict[str, str | None]] = {}  # title → record
    for title in sorted(wikitext_map.keys()):
        wikitext = wikitext_map[title]
        fields = extract_infobox_fields(wikitext, REQUIRED_FIELDS)

        if any(v is None for v in fields.values()) and title in plaintext_map:
            fields = extract_from_text(plaintext_map[title], fields, REQUIRED_FIELDS)

        record: dict[str, str | None] = {"title": title}
        record.update(fields)
        for date_field in ("birth_date", "death_date"):
            if date_field in record:
                record[date_field] = normalize_date(record[date_field])
        all_records[title] = record
    t_extract = time.monotonic() - t0
    print(f"  Extracted from {len(all_records)} articles [{t_extract:.1f}s]", flush=True)

    # 7. Write per-category Excel files
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    headers = ["title"] + list(REQUIRED_FIELDS)

    print(f"\nWriting Excel files...", flush=True)
    print(f"  {'File':30s}  {'SW match':>8s}  {'Extracted':>9s}  Path")
    per_file_extracted: dict[str, int] = {}
    for stem, file_titles in per_file_titles.items():
        sw_matched = file_titles & sw_title_set
        file_records = [all_records[t] for t in sorted(sw_matched) if t in all_records]
        per_file_extracted[stem] = len(file_records)

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = stem
        ws.append(headers)
        for r in file_records:
            ws.append([r.get(h) for h in headers])
        for col in ws.columns:
            max_len = max(len(str(cell.value or "")) for cell in col)
            ws.column_dimensions[col[0].column_letter].width = min(max_len + 2, 50)
        output_path = RESULTS_DIR / f"{stem}.xlsx"
        wb.save(output_path)
        print(f"  {stem:30s}  {len(sw_matched):8d}  {len(file_records):9d}  {output_path}")

    # 8. Summary
    t_total = time.monotonic() - t_start
    total_extracted = len(all_records)
    fill = {f: sum(1 for r in all_records.values() if r.get(f)) for f in REQUIRED_FIELDS}

    print(f"\n{'='*60}")
    print(f"Pattern files:        {len(pattern_files)}")
    print(f"Enwiki articles:      {len(all_titles)} (deduped)")
    print(f"Found in simplewiki:  {len(matched_titles)} ({len(matched_titles)/len(all_titles)*100:.1f}%)")
    print(f"Articles read:        {len(wikitext_map)}")
    print(f"Records extracted:    {total_extracted}")
    print(f"Excel files written:  {len(per_file_titles)}")

    print(f"\nTiming:")
    print(f"  Regex search:   {t_search:.1f}s")
    print(f"  Dump read:      {t_read:.1f}s")
    print(f"  Extraction:     {t_extract:.1f}s")
    print(f"  Total:          {t_total:.1f}s")

    print(f"\nField fill rates (all {total_extracted} articles):")
    for f in REQUIRED_FIELDS:
        pct = fill[f] / total_extracted * 100 if total_extracted else 0
        print(f"  {f}: {fill[f]}/{total_extracted} ({pct:.0f}%)")

    print(f"\nPer-file breakdown:")
    print(f"  {'File':30s}  {'Patterns':>8s}  {'Cats':>6s}  {'EN arts':>8s}  {'SW match':>8s}")
    for stem, n_pat, n_cats, n_arts in per_file_stats:
        sw_n = per_file_extracted.get(stem, 0)
        print(f"  {stem:30s}  {n_pat:8d}  {n_cats:6d}  {n_arts:8d}  {sw_n:8d}")


if __name__ == "__main__":
    main()
