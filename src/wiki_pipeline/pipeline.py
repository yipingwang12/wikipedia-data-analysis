"""Orchestrator: download → cache → parse → search → filter → fetch → extract → write."""

from __future__ import annotations

import logging
import time
from pathlib import Path

from dotenv import load_dotenv

from .cache import cache_dir, clear_cache, load_pickle, save_pickle
from .category_tree import (
    build_linktarget_map,
    bfs_from_root,
    collect_articles_from_categories,
    find_categories_by_regex,
    parse_category_links,
    parse_page_dump,
)
from .config import PipelineConfig, parse_args, wiki_to_lang
from .download import download_dump
from .dump_reader import (
    _article_dump_name,
    load_multistream_index,
    read_articles_from_dump,
    read_articles_multistream,
    resolve_dump_path,
    resolve_multistream_paths,
)
from .etymology_extractor import extract_etymology_fields, extract_etymology_from_lead
from .extractors import EXTRACTOR_REGISTRY
from .geo_infobox_parser import extract_geo_infobox_fields
from .infobox_parser import extract_infobox_fields
from .llm_extractor import LlmExtractor
from .nlp_extractor import extract_from_text, normalize_date_with_note
from .output import write_excel, write_results
from .page_filter import filter_pages_from_meta
from .wiki_api import WikiApiClient

logger = logging.getLogger(__name__)

def _sql_dump_files(wiki: str) -> dict[str, str]:
    return {
        "page": f"{wiki}-latest-page.sql.gz",
        "categorylinks": f"{wiki}-latest-categorylinks.sql.gz",
        "linktarget": f"{wiki}-latest-linktarget.sql.gz",
    }

CACHE_KEYS = ("catid_map", "page_meta", "lt_map", "parsed_catlinks")


def _download_and_parse(config: PipelineConfig) -> tuple[
    dict[int, str], dict[int, tuple[str, int]], dict[int, str], object
]:
    """Download dumps, load/parse caches, return (catid_map, page_meta, lt_map, parsed_catlinks)."""
    # Cache management
    if config.clear_cache:
        n = clear_cache(config.data_dir, config.wiki)
        print(f"Cleared {n} cache files", flush=True)

    use_cache = not config.no_cache
    cdir = cache_dir(config.data_dir, config.wiki)

    # Try loading caches first (works even if source dumps are deleted)
    catid_map = page_meta = lt_map = parsed_catlinks = None
    cache_hit = False

    if use_cache:
        dump_paths_for_cache = {
            key: config.data_dir / filename for key, filename in _sql_dump_files(config.wiki).items()
        }
        page_src = [dump_paths_for_cache["page"]]
        lt_src = [dump_paths_for_cache["linktarget"]]
        all_src = list(dump_paths_for_cache.values())

        catid_map = load_pickle(cdir / "catid_map.pkl", page_src)
        page_meta = load_pickle(cdir / "page_meta.pkl", page_src)
        lt_map = load_pickle(cdir / "lt_map.pkl", lt_src)
        parsed_catlinks = load_pickle(cdir / "parsed_catlinks.pkl", all_src)
        cache_hit = all(x is not None for x in (catid_map, page_meta, lt_map, parsed_catlinks))

    t0 = time.monotonic()

    if cache_hit:
        print("Loaded parsed data from cache", flush=True)
    else:
        # Download dumps only when cache miss
        dump_paths: dict[str, Path] = {}
        for key, filename in _sql_dump_files(config.wiki).items():
            url = f"{config.dump_base_url}/{filename}"
            dest = config.data_dir / filename
            dump_paths[key] = download_dump(url, dest)

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
            all_src = list(dump_paths.values())
            save_pickle(catid_map, cdir / "catid_map.pkl", page_src)
            save_pickle(page_meta, cdir / "page_meta.pkl", page_src)
            save_pickle(lt_map, cdir / "lt_map.pkl", lt_src)
            save_pickle(parsed_catlinks, cdir / "parsed_catlinks.pkl", all_src)
            print("  Saved parsed data to cache", flush=True)

    return catid_map, page_meta, lt_map, parsed_catlinks


def _search_regex(config, catid_map, parsed_catlinks):
    """Regex search: match category names, collect article IDs."""
    t0 = time.monotonic()
    all_cat_names = set(catid_map.values())
    matched_cats = find_categories_by_regex(all_cat_names, config.category_patterns)
    article_ids = collect_articles_from_categories(parsed_catlinks, matched_cats)
    t1 = time.monotonic()
    print(f"  {len(matched_cats)} categories matched {len(config.category_patterns)} pattern(s)", flush=True)
    print(f"  {len(article_ids)} article IDs [{t1 - t0:.1f}s]", flush=True)
    return article_ids


def _search_bfs(config, parsed_catlinks):
    """BFS search: traverse from root category."""
    t0 = time.monotonic()
    root_cat = config.root_category.replace(" ", "_")
    tree = bfs_from_root(parsed_catlinks, root_cat, max_depth=config.max_depth)
    t1 = time.monotonic()
    print(f"  {len(tree.article_ids)} article IDs in category tree [{t1 - t0:.1f}s]", flush=True)
    print(f"  {sum(len(v) for v in tree.subcategories.values())} subcategory links", flush=True)

    if tree.depth_stats:
        print("  Depth | Categories | New articles", flush=True)
        cum_articles = 0
        for depth, (cats, arts) in enumerate(tree.depth_stats):
            cum_articles += arts
            print(f"  {depth:5d} | {cats:10d} | {arts:12d}  (cum: {cum_articles})", flush=True)

    return tree.article_ids


def _output_name(config: PipelineConfig) -> str:
    """Derive output filename stem from config."""
    if config.root_category:
        return config.root_category.replace(" ", "_")
    return "regex_results"


def _load_multistream_index(
    config: PipelineConfig, index_path: Path
) -> dict[int, int]:
    """Load multistream index, using pickle cache when available."""
    use_cache = not config.no_cache
    cdir = cache_dir(config.data_dir, config.wiki)
    if use_cache:
        cached = load_pickle(cdir / "multistream_index.pkl", [index_path])
        if cached is not None:
            print(f"  Loaded multistream index from cache ({len(cached)} pages)", flush=True)
            return cached
    index = load_multistream_index(index_path)
    if use_cache:
        save_pickle(index, cdir / "multistream_index.pkl", [index_path])
        print("  Saved multistream index to cache", flush=True)
    return index


def _download_articles_dump(config: PipelineConfig) -> Path:
    """Download article content dump. Multistream by default."""
    if config.use_multistream:
        index_name = _article_dump_name(config.wiki, "multistream_index")
        index_url = f"{config.dump_base_url}/{index_name}"
        index_dest = config.data_dir / index_name
        print(f"Downloading multistream index: {index_name}", flush=True)
        download_dump(index_url, index_dest)

        dump_name = _article_dump_name(config.wiki, "multistream")
        dump_url = f"{config.dump_base_url}/{dump_name}"
        dump_dest = config.data_dir / dump_name
        print(f"Downloading multistream dump: {dump_name}", flush=True)
        return download_dump(dump_url, dump_dest)
    else:
        legacy_name = _article_dump_name(config.wiki, "bz2")
        url = f"{config.dump_base_url}/{legacy_name}"
        dest = config.data_dir / legacy_name
        print(f"Downloading article content dump: {legacy_name}", flush=True)
        return download_dump(url, dest)


def run(config: PipelineConfig) -> Path | None:
    """Execute the full pipeline, return output path (or None for dry-run)."""
    load_dotenv()

    if config.download_articles:
        path = _download_articles_dump(config)
        print(f"Article dump saved to {path}")
        if not config.root_category and not config.category_patterns:
            return None

    catid_map, page_meta, lt_map, parsed_catlinks = _download_and_parse(config)

    # Search phase: regex or BFS
    t0 = time.monotonic()
    if config.search_mode == "regex":
        article_ids = _search_regex(config, catid_map, parsed_catlinks)
    else:
        article_ids = _search_bfs(config, parsed_catlinks)

    # Filter pages from cached metadata
    pages = filter_pages_from_meta(page_meta, article_ids, config.min_page_length)
    t_filt = time.monotonic()
    print(f"  {len(pages)} articles (len>={config.min_page_length}) [{t_filt - t0:.1f}s]", flush=True)
    print(f"  Total time: {t_filt - t0:.1f}s", flush=True)

    if config.dry_run:
        est_nlp = int(len(pages) * 0.3)
        est_llm = int(len(pages) * 0.1)
        est_cost = est_llm * 0.001
        print(f"Dry run — estimated NLP: ~{est_nlp}, LLM: ~{est_llm}, cost: ~${est_cost:.2f}")
        return None

    # Fetch content: local dump (default) or API
    titles = [p.title.replace("_", " ") for p in pages]
    title_to_id = {p.title.replace("_", " "): p.page_id for p in pages}

    if config.use_api:
        api = WikiApiClient(
            api_url=config.wiki_api_url,
            batch_size=config.api_batch_size,
            rate_limit_s=config.api_rate_limit_s,
        )
        wikitext_map = api.fetch_wikitext_batch(titles)
        plaintext_map = api.fetch_plaintext_batch(titles)
    else:
        page_id_set = {p.page_id for p in pages}
        if config.use_multistream:
            dump_path, index_path = resolve_multistream_paths(config.data_dir, config.wiki)
            index = _load_multistream_index(config, index_path)
            print(f"  Reading via multistream index ({len(page_id_set)} articles)", flush=True)
            wikitext_map, plaintext_map = read_articles_multistream(
                dump_path, index, page_id_set, limit=config.limit,
            )
        else:
            dump_path = resolve_dump_path(config.data_dir, config.wiki)
            print(f"  Reading from {dump_path.name}", flush=True)
            wikitext_map, plaintext_map = read_articles_from_dump(
                dump_path, page_id_set, limit=config.limit,
            )

    # Resolve extractor function for a given mode or pattern file stem
    _MODE_EXTRACTORS = {
        "bio": extract_infobox_fields,
        "geo": extract_geo_infobox_fields,
        "etymology": extract_etymology_fields,
    }
    for stem, (fn, _) in EXTRACTOR_REGISTRY.items():
        # Map pattern stems to extraction mode names
        _stem_to_mode = {
            "battles_wars_conflicts": "battle",
            "explorations_voyages_spacecraft": "exploration",
            "astronomy": "astronomy",
            "biology": "biology",
            "mathematics_statistics": "math",
        }
    for stem, mode in _stem_to_mode.items():
        _MODE_EXTRACTORS[mode] = EXTRACTOR_REGISTRY[stem][0]

    _MODE_FIELDS = {
        "bio": ("birth_date", "death_date", "nationality", "occupation"),
        "geo": ("population", "area_km2", "elevation_m", "subdivision_name", "subdivision_type"),
        "etymology": ("etymology",),
    }
    for stem, (_, fields) in EXTRACTOR_REGISTRY.items():
        _MODE_FIELDS[_stem_to_mode[stem]] = fields

    _STEM_TO_MODE = {
        **_stem_to_mode,
        **{s: "bio" for s in (
            "athletes", "businesspeople", "explorers", "historians",
            "movie_directors_actors", "musicians_composers", "philosophers",
            "photographers", "politicians_rulers", "religious_figures",
            "scientists", "visual_artists", "writers_authors",
        )},
        **{s: "geo" for s in (
            "admin_subdivisions", "populated_places", "geographic_features",
            "parks_protected", "transportation", "buildings_structures",
            "historical_cultural", "demographics_economy",
        )},
    }

    _DATE_FIELDS = {"birth_date", "death_date", "date", "discovery_date", "year_discovered"}

    def _extract_article(title, extract_fn, required_fields, mode="bio"):
        wikitext = wikitext_map.get(title, "")
        plaintext = plaintext_map.get(title, "")
        fields = extract_fn(wikitext, required_fields)

        if mode == "etymology":
            if fields.get("etymology") is None and plaintext:
                lead_text = extract_etymology_from_lead(plaintext)
                if lead_text:
                    fields["etymology"] = lead_text
            if fields.get("etymology") is None and plaintext and not config.no_llm:
                fields = llm.extract_etymology(plaintext, lang=wiki_to_lang(config.wiki))
        else:
            has_gaps = any(v is None for v in fields.values())
            if has_gaps and plaintext:
                fields = extract_from_text(plaintext, fields, required_fields)
                has_gaps = any(v is None for v in fields.values())
                if has_gaps and not config.no_llm:
                    fields = llm.extract_missing(
                        plaintext, fields, required_fields,
                        lang=wiki_to_lang(config.wiki),
                    )

        date_notes: dict[str, str | None] = {}
        for date_field in _DATE_FIELDS:
            if date_field in fields:
                if fields[date_field]:
                    normalized, note = normalize_date_with_note(fields[date_field])
                    fields[date_field] = normalized
                    date_notes[f"{date_field}_note"] = note
                else:
                    date_notes[f"{date_field}_note"] = None

        record: dict[str, str | int | None] = {
            "page_id": title_to_id.get(title, 0),
            "title": title,
        }
        record.update(fields)
        record.update(date_notes)
        record["article_bytes"] = title_to_length.get(title, 0)
        record["word_count"] = len(plaintext.split()) if plaintext else 0
        return record

    llm = LlmExtractor(model=config.claude_model)
    title_to_length = {p.title.replace("_", " "): p.length for p in pages}

    # Per-pattern-group extraction + output (auto mode with groups)
    if config.pattern_groups and config.output_format == "xlsx":
        all_cat_names = set(catid_map.values())
        output_paths: list[Path] = []
        total_processed = 0

        for stem, group_patterns in config.pattern_groups:
            mode = _STEM_TO_MODE.get(stem, config.extraction_mode)
            if mode == "auto":
                mode = "bio"
            extract_fn = _MODE_EXTRACTORS.get(mode, extract_infobox_fields)
            required_fields = _MODE_FIELDS.get(mode, config.required_fields)

            matched_cats = find_categories_by_regex(all_cat_names, group_patterns)
            group_ids = collect_articles_from_categories(parsed_catlinks, matched_cats)
            group_pages = filter_pages_from_meta(page_meta, group_ids, config.min_page_length)
            group_titles = sorted({p.title.replace("_", " ") for p in group_pages} & set(wikitext_map.keys()))

            group_records = [_extract_article(t, extract_fn, required_fields, mode) for t in group_titles]
            total_processed += len(group_records)

            output_path = config.results_dir / f"{stem}.xlsx"
            write_excel(group_records, output_path, sheet_name=stem)
            output_paths.append(output_path)
            print(f"  {stem} [{mode}]: {len(group_records)} articles → {output_path}")

        print(f"Processed {total_processed} articles across {len(output_paths)} Excel files in {config.results_dir}")
        return config.results_dir
    else:
        # Single-mode extraction
        mode = config.extraction_mode if config.extraction_mode != "auto" else "bio"
        extract_fn = _MODE_EXTRACTORS.get(mode, extract_infobox_fields)
        all_records: list[dict[str, str | int | None]] = []
        for title in titles:
            all_records.append(_extract_article(title, extract_fn, config.required_fields, mode))
        print(f"Processed {len(all_records)} articles")

        ext = {"xlsx": "xlsx", "tsv": "tsv"}.get(config.output_format, "csv")
        stem = _output_name(config)
        output_path = config.results_dir / f"{stem}.{ext}"
        result = write_results(all_records, output_path, config.output_format)
        print(f"Results written to {result}")
        return result


def main() -> None:
    logging.basicConfig(level=logging.WARNING)
    config = parse_args()
    run(config)


if __name__ == "__main__":
    main()
