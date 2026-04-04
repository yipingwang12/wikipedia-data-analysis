# Wikipedia Category Data Pipeline

Extract structured data from Wikipedia articles found via regex category search or BFS traversal. Two extraction modes: biographical (birth/death dates, nationality, occupation) and geographic (population, area, elevation, subdivision).

## Search Modes

### Regex (default) — match category names by pattern
Search all ~2.6M Wikipedia category names using regular expressions (case-insensitive). Define a list of patterns to capture categories related to a theme, then collect all non-stub articles from matched categories.

### BFS — traverse from a root category
Traditional BFS from a single root category with optional `--max-depth` limit. Prints per-depth diagnostics.

## Pipeline Stages

1. **Download** — 3 SQL dumps from Wikimedia (~5.7 GB compressed): `page`, `categorylinks`, `linktarget`
2. **Parse & Cache** — Single-pass page dump (catid_map + page_meta), linktarget map, full category adjacency. Cached as pickle with mtime-validated sidecars in `data/.cache/` (~1.8 GB)
3. **Search** — Regex pattern matching on category names (default) OR BFS traversal from root category
4. **Filter** — In-memory filter by length/namespace from cached page_meta (default min 5000 bytes, excludes stubs)
5. **Fetch** — Batch wikitext + plaintext via MediaWiki API
6. **Extract** — Infobox parsing (primary) → NLP regex extraction (secondary, bio mode only) → Claude LLM fallback (tertiary)
7. **Output** — CSV/TSV with page_id, title, and extracted fields
8. **Transform** (optional) — CSV → `wikipedia.json` keyed by GADM GID_2 for map integration

## Usage

```bash
# Regex mode (default) — match category names by pattern
wiki-pipeline --patterns "French.*paint" "Italian.*paint" --dry-run
wiki-pipeline --patterns-file patterns/visual_artists.txt --dry-run
wiki-pipeline --patterns "paint" "sculpt" "drawing"

# BFS mode — traverse from root category
wiki-pipeline French_painters --dry-run
wiki-pipeline Painters_by_nationality --dry-run
wiki-pipeline "Writers by nationality" --dry-run --max-depth 2

# Geographic mode — extract settlement/county data
wiki-pipeline --patterns-file patterns/geo/admin_subdivisions.txt --extraction-mode geo --dry-run
wiki-pipeline --patterns-file patterns/geo/populated_places.txt --extraction-mode geo

# Integration: pipeline → transform → map app data
python scripts/run_geo_integration.py --country us --map-data-dir /path/to/global-geo-atlas/data
python scripts/transform_to_gadm.py --csv results/geo/regex_results.csv --gadm-data-dir /path/to/data/us --output /path/to/data/us/wikipedia.json
```

### Flags

- `--wiki NAME` — wiki project (default: simplewiki; use `enwiki` for full English Wikipedia)
- `--patterns REGEX [REGEX ...]` — regex patterns to match category names (case-insensitive)
- `--patterns-file PATH` — file with one regex pattern per line (combinable with `--patterns`)
- `--extraction-mode bio|geo` — biographical (default) or geographic field extraction
- `--required-fields FIELD [...]` — override default fields (auto-set by extraction mode)
- `--max-depth N` — limit BFS to N levels below root (default: unlimited)
- `--no-multistream` — use legacy full-scan dump reader instead of multistream index
- `--no-cache` — bypass cache read/write, force full re-parse
- `--clear-cache` — delete cached files before run

## Pattern Files

Pre-built pattern files in `patterns/` for 13 categories of notable people:

| File | Patterns | Categories | Articles | Non-stub |
|---|---|---|---|---|
| athletes.txt | 46 | 62,829 | 604,218 | 272,563 |
| politicians_rulers.txt | 29 | 51,348 | 514,626 | 230,278 |
| writers_authors.txt | 22 | 18,894 | 436,000 | 244,028 |
| musicians_composers.txt | 30 | 22,275 | 389,308 | 102,411 |
| movie_directors_actors.txt | 13 | 15,302 | 281,760 | 108,668 |
| scientists.txt | 40 | 10,964 | 178,222 | 80,166 |
| religious_figures.txt | 47 | 26,797 | 169,404 | 81,780 |
| businesspeople.txt | 9 | 4,102 | 84,731 | 56,052 |
| visual_artists.txt | 23 | 5,583 | 67,246 | 35,127 |
| historians.txt | 15 | 2,532 | 40,007 | 24,847 |
| philosophers.txt | 9 | 1,317 | 24,908 | 9,410 |
| photographers.txt | 2 | 1,073 | 13,110 | 8,553 |
| explorers.txt | 6 | 857 | 10,348 | 6,154 |
| **Total (deduplicated)** | | **217,836** | **2,471,918** | **1,034,547** |

Biographical patterns use word-boundary anchors (`(?:^|_)..(?:$|_)`). Geographic patterns use `(?:_in_|_of_)` suffixes matching Wikipedia's `Things_in_Place` convention.

### Geographic Patterns (`patterns/geo/`)

| File | Patterns | Purpose |
|---|---|---|
| admin_subdivisions.txt | 20 | Counties, municipalities, districts |
| populated_places.txt | 12 | Cities, towns, county seats |
| geographic_features.txt | 18 | Rivers, mountains, lakes, valleys |
| parks_protected.txt | 9 | National parks, nature reserves |
| transportation.txt | 12 | Airports, highways, railways |
| buildings_structures.txt | 10 | Universities, museums, stadiums |
| historical_cultural.txt | 16 | Historic sites, castles, UNESCO |
| demographics_economy.txt | 8 | Economy, demographics, companies |

## Architecture

```
src/wiki_pipeline/
  config.py          # frozen dataclass + argparse CLI, search_mode/extraction_mode/wiki
  download.py        # streaming download w/ resume + exponential backoff
  sql_parser.py      # streaming MySQL INSERT parser (C-speed str.find)
  category_tree.py   # parse_page_dump, parse_category_links, bfs_from_root,
                     # find_categories_by_regex, collect_articles_from_categories
  page_filter.py     # filter_pages (streaming) + filter_pages_from_meta (cached)
  cache.py           # pickle save/load with .meta mtime sidecar
  dump_reader.py     # multistream index reader (default), legacy full-scan fallback
  pipeline.py        # cache-aware orchestrator (regex + BFS modes)
  wiki_api.py        # MediaWiki API client (batched, rate-limited)
  infobox_parser.py      # mwparserfromhell biographical field extraction
  geo_infobox_parser.py  # mwparserfromhell geographic field extraction
  nlp_extractor.py       # regex extraction from first-sentence biographical patterns
  llm_extractor.py       # Claude Haiku fallback for missing fields
  output.py              # CSV/TSV writer

scripts/
  transform_to_gadm.py    # CSV → wikipedia.json keyed by GADM GID_2
  run_geo_integration.py   # orchestrator: pipeline → transform → map data dir

patterns/              # biographical regex pattern files
patterns/geo/          # geographic regex pattern files
results/               # output directory for pipeline results
```

## Performance

With cached dumps (~21s load time):
- Regex search: ~21s for 23 patterns × 2.6M category names
- Article collection + filtering: <0.1s
- Total cached run: ~42s regardless of pattern count

Initial parse (cold cache): ~30 min (one-time cost, shared across all searches)

### Content sourcing benchmarks

**Multistream index reader** (2026-04-03, simplewiki ~358 MB dump):
- Uses Wikimedia's multistream bz2 format: independently-compressed blocks with byte-offset index
- Index load: ~0.5s (551K pages); cached as pickle for instant reload
- Seeks directly to needed blocks, decompresses only target pages
- 10 articles: 0.06s; 1,375 articles: 23s; 69,126 articles: 357s (176/s)
- No decompression to disk required — reads directly from compressed dump

**Full pipeline run** (2026-04-03, all 13 bio patterns → simplewiki):
- Cache load: ~31s; regex search (298 patterns): ~292s; multistream read: ~357s; extraction: ~330s
- Total: ~17 min for 69,126 articles (6.6% of 1,046,545 enwiki matches found in simplewiki)
- Fill rates: birth_date 70%, death_date 35%, nationality 68%, occupation 55%

**Legacy full-scan reader** (2026-04-01, enwiki 106 GB uncompressed):
- `iterparse` on 106 GB is CPU-bound; 1000-article run did not complete in ~10 min
- Retained as `--no-multistream` fallback

**API fetch** (estimated from rate limits):
- ~10–20 articles/sec (50/batch, 0.1s rate limit, 2 passes for wikitext+plaintext)
- 1K articles: ~2 min; 35K: ~30–60 min; 1M: ~14–28 hrs
- Scales linearly with article count; efficient for small runs

**Disk usage** (data/):
- SQL dump caches: ~1.8 GB (catid_map, page_meta, lt_map, parsed_catlinks)
- Multistream dump (simplewiki): ~358 MB + ~5 MB index
- Multistream dump (enwiki): ~22 GB + ~250 MB index
- Legacy article dump (enwiki bz2): ~23 GB; uncompressed: ~106 GB

## Tests

263 tests. Run: `.venv/bin/python -m pytest tests/ -v`

## Extraction Modes

### Biographical (`--extraction-mode bio`, default)

Three-tier field extraction for birth_date, death_date, nationality, occupation:

1. **Infobox parsing** — `mwparserfromhell` extracts from structured Wikipedia infoboxes with field alias support (e.g., `born` → `birth_date`). Handles `{{birth date}}` templates, wikilink/ref stripping.
2. **NLP regex extraction** — pattern-based extraction from the first paragraph. Exploits Wikipedia's consistent biographical first-sentence convention (`Name (DATE – DATE) was a [nationality] [occupation]`). Handles date normalization to ISO, "or" dates (picks earliest birth/latest death), parentheticals with place names, `(born DATE – DATE)` patterns, compound nationalities ("Polish and naturalised-French"), qualified occupations ("theoretical physicist"), and "of [nationality] origin/descent" postfix patterns. Uses ~200 nationality and ~150 occupation lookup entries. Zero external dependencies.
3. **Claude LLM fallback** — sends truncated plaintext to Claude Haiku for remaining gaps. Requires Anthropic API key (separate billing from Claude subscription).

### Extraction benchmarks (1000 random visual artist articles)

| Source | Count |
|---|---|
| infobox only | 37 |
| infobox+nlp | 381 |
| nlp only | 557 |
| none | 25 |

| Field | Fill rate |
|---|---|
| birth_date | 88% |
| death_date | 59% |
| nationality | 86% |
| occupation | 94% |

NLP tier contributed on 94% of articles. The 25 "none" results were list pages and redirects. LLM tier was not invoked in benchmarking.

### Geographic (`--extraction-mode geo`)

Extracts population, area_km2, elevation_m, subdivision_name, subdivision_type from settlement/city/county/district/place infoboxes. Same three-tier cascade (geo infobox → NLP skipped → LLM fallback). Field aliases map common infobox variants (e.g., `population_total`, `pop` → `population`).

## Map Integration

Transform pipeline output to GADM-compatible JSON for the [global-geo-atlas](https://github.com/yipingwang12/global-geo-atlas) map app.

**Join strategy**: normalize Wikipedia article titles and GADM NAME_2+NAME_1 fields, strip admin suffixes (County, Parish, Borough), match case-insensitively. Unmatched rows logged to `unmatched.csv`.

**Output format** (`wikipedia.json`):
```json
{
  "USA.14.17_1": {
    "title": "Cook County, Illinois",
    "population": "5275541",
    "area_km2": "2448",
    "wikipedia_url": "https://en.wikipedia.org/wiki/Cook_County,_Illinois"
  }
}
```

### Map App Changes

Implemented on the `feature/wikipedia-integration` branch of [global-geo-atlas](https://github.com/yipingwang12/global-geo-atlas):

1. **DataLoader.js** — `loadWikipediaData()` fetch with `{}` fallback, `getWikipediaData(gid2)` accessor, reset in `setCountry()`
2. **gadm-defaults.js** — `wikipediaData: /data/${meta.id}/wikipedia.json` in `buildGadmConfig()`
3. **MapRenderer.js** — population, area, Wikipedia link in hover tooltip via `getWikipediaData(id)`
4. **main.js** — `loadWikipediaData()` called on country switch alongside metro/city data

Graceful degradation: missing `wikipedia.json` returns `{}`, missing keys return `null`, tooltip rows simply omitted.

### Future: Unified Monorepo

Goal: run both pipelines (Wikipedia extraction + map app) from a single directory with shared orchestration. Proposed structure:

```
wikipedia-maps/
├── wikipedia-pipeline/      # this repo (git subtree)
├── global-geo-atlas/        # map app repo (git subtree)
└── integration/
    ├── config.py            # per-country settings: pattern file, join fields, GADM paths
    └── run.py               # orchestrator: pipeline → transform → place per country
```

`run.py` would iterate countries from `config.py`, invoke the Wikipedia pipeline with the appropriate geo pattern file, transform CSV → `wikipedia.json`, and place output in the map app's `data/{country-id}/` directory. Supports `--country {id}` for single-country runs and `--all` for batch.

## Key Design Decisions

- **Regex as default search** — scan all category names with user-defined patterns; more flexible than single-root BFS for thematic queries
- **BFS retained as option** — useful when category hierarchy matters or a clean root exists
- **Cache is search-independent** — dump parsing is global; only search (regex/BFS) depends on user input
- **Single-pass page dump** — produces both catid_map (ns=14) and page_meta (ns=0, non-redirect) in one pass
- **Mtime sidecar validation** — `.meta` files track source dump mtimes; stale cache auto-invalidated without loading full pickle
- **Streaming SQL parser** — `str.find()` scanning for multi-hundred-MB INSERT lines, O(1) memory per row
- **Multistream index reader** — seeks directly into compressed bz2 blocks via byte-offset index; reads only target pages instead of scanning entire dump. Default over legacy full-scan.
- **Wiki-agnostic filenames** — dump filenames parameterized by `--wiki` (e.g., simplewiki, enwiki); SQL dump caches are wiki-independent
- **Three-tier extraction** — infobox → NLP regex → LLM fallback → None; NLP tier eliminates most LLM calls at zero cost
- **Word-boundary anchored patterns** — prevent false positives from substring matches (e.g., `count` in "county")
- **Dual extraction modes** — bio/geo share pipeline infrastructure; only infobox parser and default fields differ
- **GADM join by title normalization** — avoids external dependency on Wikidata; suffix stripping handles County/Parish/Borough variants
