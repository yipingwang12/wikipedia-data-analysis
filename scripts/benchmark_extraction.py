"""Benchmark: extract biographical data for 1000 random artists from local dump."""

from __future__ import annotations

import pickle
import random
import time
from pathlib import Path

from wiki_pipeline.category_tree import (
    collect_articles_from_categories,
    find_categories_by_regex,
    ParsedCategoryLinks,
)
from wiki_pipeline.dump_reader import read_articles_from_dump
from wiki_pipeline.infobox_parser import extract_infobox_fields
from wiki_pipeline.nlp_extractor import extract_from_text
from wiki_pipeline.output import write_results
from wiki_pipeline.page_filter import filter_pages_from_meta, PageInfo

SAMPLE_SIZE = 1000
MIN_PAGE_LENGTH = 5000
REQUIRED_FIELDS = ("birth_date", "death_date", "nationality", "occupation")
DATA_DIR = Path("data")
CACHE_DIR = DATA_DIR / ".cache"
DUMP_FILE = DATA_DIR / "enwiki-latest-pages-articles.xml.bz2"
PATTERNS_FILE = Path("patterns/visual_artists.txt")
RESULTS_DIR = Path("results/benchmark_extraction")


def load_cache(name: str) -> object:
    path = CACHE_DIR / f"{name}.pkl"
    with open(path, "rb") as f:
        return pickle.load(f)  # noqa: S301


def main() -> None:
    t_start = time.monotonic()

    # Load caches
    print("Loading caches...", flush=True)
    t0 = time.monotonic()
    catid_map: dict[int, str] = load_cache("catid_map")
    page_meta: dict[int, tuple[str, int]] = load_cache("page_meta")
    parsed_catlinks: ParsedCategoryLinks = load_cache("parsed_catlinks")
    t_cache = time.monotonic() - t0
    print(f"  Loaded caches [{t_cache:.1f}s]", flush=True)

    # Find artist categories
    print("Searching for artist categories...", flush=True)
    t0 = time.monotonic()
    patterns = [
        line.strip()
        for line in PATTERNS_FILE.read_text().splitlines()
        if line.strip()
    ]
    all_cat_names = set(catid_map.values())
    matched_cats = find_categories_by_regex(all_cat_names, patterns)
    article_ids = collect_articles_from_categories(parsed_catlinks, matched_cats)
    t_search = time.monotonic() - t0
    print(f"  {len(matched_cats)} categories, {len(article_ids)} articles [{t_search:.1f}s]", flush=True)

    # Filter and sample
    pages = filter_pages_from_meta(page_meta, article_ids, MIN_PAGE_LENGTH)
    print(f"  {len(pages)} non-stub articles", flush=True)

    random.seed(42)
    sample: list[PageInfo] = random.sample(pages, min(SAMPLE_SIZE, len(pages)))
    print(f"  Sampled {len(sample)} articles", flush=True)

    # Read from dump
    print("Reading articles from dump...", flush=True)
    t0 = time.monotonic()
    page_id_set = {p.page_id for p in sample}
    wikitext_map, plaintext_map = read_articles_from_dump(DUMP_FILE, page_id_set)
    t_dump = time.monotonic() - t0
    print(f"  Read {len(wikitext_map)} articles from dump [{t_dump:.1f}s]", flush=True)

    # Extract fields: infobox → NLP (no LLM for benchmark)
    print("Extracting biographical fields...", flush=True)
    t0 = time.monotonic()
    title_to_id = {p.title.replace("_", " "): p.page_id for p in sample}
    records: list[dict[str, str | int | None]] = []
    infobox_only = 0
    infobox_nlp = 0
    nlp_only = 0
    none_count = 0

    for page in sample:
        title = page.title.replace("_", " ")
        wikitext = wikitext_map.get(title, "")
        fields = extract_infobox_fields(wikitext, REQUIRED_FIELDS)

        infobox_had = any(v is not None for v in fields.values())
        has_gaps = any(v is None for v in fields.values())

        if has_gaps and title in plaintext_map:
            fields = extract_from_text(plaintext_map[title], fields, REQUIRED_FIELDS)

        nlp_had = any(v is not None for v in fields.values()) and has_gaps

        if infobox_had and not nlp_had:
            infobox_only += 1
        elif infobox_had and nlp_had:
            infobox_nlp += 1
        elif nlp_had:
            nlp_only += 1
        else:
            none_count += 1

        record: dict[str, str | int | None] = {"page_id": page.page_id, "title": title}
        record.update(fields)
        records.append(record)

    t_extract = time.monotonic() - t0
    print(f"  Extracted [{t_extract:.1f}s]", flush=True)

    # Write results
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    output_path = RESULTS_DIR / "artists_sample_1000.csv"
    write_results(records, output_path, "csv")
    print(f"  Written to {output_path}", flush=True)

    # Report
    t_total = time.monotonic() - t_start
    print(f"\n{'='*50}")
    print(f"Timing:")
    print(f"  Cache load:  {t_cache:.1f}s")
    print(f"  Search:      {t_search:.1f}s")
    print(f"  Dump read:   {t_dump:.1f}s")
    print(f"  Extraction:  {t_extract:.1f}s")
    print(f"  Total:       {t_total:.1f}s")
    print(f"\nExtraction sources:")
    print(f"  Infobox only:  {infobox_only}")
    print(f"  Infobox + NLP: {infobox_nlp}")
    print(f"  NLP only:      {nlp_only}")
    print(f"  None:          {none_count}")

    fill_rates: dict[str, int] = {f: 0 for f in REQUIRED_FIELDS}
    for r in records:
        for f in REQUIRED_FIELDS:
            if r.get(f) is not None:
                fill_rates[f] += 1
    print(f"\nField fill rates ({len(records)} articles):")
    for f, count in fill_rates.items():
        print(f"  {f}: {count}/{len(records)} ({count/len(records)*100:.0f}%)")


if __name__ == "__main__":
    main()
