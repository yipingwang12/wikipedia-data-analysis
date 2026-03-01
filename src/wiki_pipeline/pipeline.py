"""Orchestrator: download → cache → parse → BFS → filter → fetch → extract → write."""

from __future__ import annotations

import logging
import time
from pathlib import Path

from dotenv import load_dotenv

from .cache import cache_dir, clear_cache, load_pickle, save_pickle
from .category_tree import (
    build_linktarget_map,
    bfs_from_root,
    parse_category_links,
    parse_page_dump,
)
from .config import PipelineConfig, parse_args
from .download import download_dump
from .infobox_parser import extract_infobox_fields
from .llm_extractor import LlmExtractor
from .output import write_results
from .page_filter import filter_pages_from_meta
from .wiki_api import WikiApiClient

logger = logging.getLogger(__name__)

DUMP_FILES = {
    "page": "enwiki-latest-page.sql.gz",
    "categorylinks": "enwiki-latest-categorylinks.sql.gz",
    "linktarget": "enwiki-latest-linktarget.sql.gz",
}

CACHE_KEYS = ("catid_map", "page_meta", "lt_map", "parsed_catlinks")


def run(config: PipelineConfig) -> Path | None:
    """Execute the full pipeline, return output path (or None for dry-run)."""
    load_dotenv()

    # Download dumps
    dump_paths: dict[str, Path] = {}
    for key, filename in DUMP_FILES.items():
        url = f"{config.dump_base_url}/{filename}"
        dest = config.data_dir / filename
        dump_paths[key] = download_dump(url, dest)

    # Cache management
    if config.clear_cache:
        n = clear_cache(config.data_dir)
        print(f"Cleared {n} cache files", flush=True)

    use_cache = not config.no_cache
    cdir = cache_dir(config.data_dir)

    # Try loading all caches
    catid_map = page_meta = lt_map = parsed_catlinks = None
    cache_hit = False

    if use_cache:
        page_src = [dump_paths["page"]]
        lt_src = [dump_paths["linktarget"]]
        all_src = [dump_paths["page"], dump_paths["linktarget"], dump_paths["categorylinks"]]

        catid_map = load_pickle(cdir / "catid_map.pkl", page_src)
        page_meta = load_pickle(cdir / "page_meta.pkl", page_src)
        lt_map = load_pickle(cdir / "lt_map.pkl", lt_src)
        parsed_catlinks = load_pickle(cdir / "parsed_catlinks.pkl", all_src)
        cache_hit = all(x is not None for x in (catid_map, page_meta, lt_map, parsed_catlinks))

    t0 = time.monotonic()

    if cache_hit:
        print("Loaded parsed data from cache", flush=True)
    else:
        print("Parsing page dump (single pass)...", flush=True)
        catid_map, page_meta = parse_page_dump(str(dump_paths["page"]))
        t1 = time.monotonic()
        print(f"  {len(catid_map)} categories, {len(page_meta)} articles [{t1 - t0:.1f}s]", flush=True)

        print("Building linktarget map...", flush=True)
        lt_map = build_linktarget_map(str(dump_paths["linktarget"]))
        t2 = time.monotonic()
        print(f"  {len(lt_map)} category linktargets [{t2 - t1:.1f}s]", flush=True)

        print("Parsing category links...", flush=True)
        parsed_catlinks = parse_category_links(
            str(dump_paths["categorylinks"]), catid_map, lt_to_name=lt_map
        )
        t3 = time.monotonic()
        print(f"  {len(parsed_catlinks.children)} parent categories [{t3 - t2:.1f}s]", flush=True)

        if use_cache:
            page_src = [dump_paths["page"]]
            lt_src = [dump_paths["linktarget"]]
            all_src = [dump_paths["page"], dump_paths["linktarget"], dump_paths["categorylinks"]]
            save_pickle(catid_map, cdir / "catid_map.pkl", page_src)
            save_pickle(page_meta, cdir / "page_meta.pkl", page_src)
            save_pickle(lt_map, cdir / "lt_map.pkl", lt_src)
            save_pickle(parsed_catlinks, cdir / "parsed_catlinks.pkl", all_src)
            print("  Saved parsed data to cache", flush=True)

    # BFS from root (milliseconds)
    tree = bfs_from_root(parsed_catlinks, config.root_category)
    t_bfs = time.monotonic()
    print(f"  {len(tree.article_ids)} article IDs in category tree [{t_bfs - t0:.1f}s]", flush=True)
    print(f"  {sum(len(v) for v in tree.subcategories.values())} subcategory links", flush=True)

    # Filter pages from cached metadata
    pages = filter_pages_from_meta(page_meta, tree.article_ids, config.min_page_length)
    t_filt = time.monotonic()
    print(f"  {len(pages)} articles (len>={config.min_page_length}) [{t_filt - t_bfs:.1f}s]", flush=True)
    print(f"  Total time: {t_filt - t0:.1f}s", flush=True)

    if config.dry_run:
        est_calls = int(len(pages) * 0.3)
        est_cost = est_calls * 0.001
        print(f"Dry run — estimated LLM calls: ~{est_calls}, cost: ~${est_cost:.2f}")
        return None

    # Fetch content via API
    api = WikiApiClient(
        api_url=config.wiki_api_url,
        batch_size=config.api_batch_size,
        rate_limit_s=config.api_rate_limit_s,
    )
    titles = [p.title.replace("_", " ") for p in pages]
    title_to_id = {p.title.replace("_", " "): p.page_id for p in pages}

    wikitext_map = api.fetch_wikitext_batch(titles)
    plaintext_map = api.fetch_plaintext_batch(titles)

    # Extract fields
    llm = LlmExtractor(model=config.claude_model)
    records: list[dict[str, str | int | None]] = []
    llm_calls = 0

    for title in titles:
        wikitext = wikitext_map.get(title, "")
        fields = extract_infobox_fields(wikitext, config.required_fields)

        has_gaps = any(v is None for v in fields.values())
        if has_gaps and title in plaintext_map:
            fields = llm.extract_missing(
                plaintext_map[title], fields, config.required_fields
            )
            llm_calls += 1

        record: dict[str, str | int | None] = {
            "page_id": title_to_id.get(title, 0),
            "title": title,
        }
        record.update(fields)
        records.append(record)

    print(f"Processed {len(records)} articles, {llm_calls} LLM fallback calls")

    # Write output
    ext = "tsv" if config.output_format == "tsv" else "csv"
    output_path = config.results_dir / f"{config.root_category.replace(' ', '_')}.{ext}"
    result = write_results(records, output_path, config.output_format)
    print(f"Results written to {result}")
    return result


def main() -> None:
    logging.basicConfig(level=logging.WARNING)
    config = parse_args()
    run(config)


if __name__ == "__main__":
    main()
