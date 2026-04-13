# Wikipedia Category Data Pipeline

Extract structured data from Wikipedia articles found via regex category search or BFS traversal. Two extraction modes: biographical (birth/death dates, nationality, occupation) and geographic (population, area, elevation, subdivision).

## Search Modes

### Regex (default) — match category names by pattern
Search all ~2.6M Wikipedia category names using regular expressions (case-insensitive). Define a list of patterns to capture categories related to a theme, then collect all non-stub articles from matched categories.

### BFS — traverse from a root category
Traditional BFS from a single root category with optional `--max-depth` limit. Prints per-depth diagnostics.

## Pipeline Stages

1. **Download** — 3 SQL dumps from Wikimedia (~5.7 GB compressed): `page`, `categorylinks`, `linktarget`
2. **Parse & Cache** — Single-pass page dump (catid_map + page_meta), linktarget map, full category adjacency. Cached as pickle with mtime-validated sidecars in `data/.cache/{wiki}/` (~1.8 GB for enwiki)
3. **Search** — Regex pattern matching on category names (default) OR BFS traversal from root category
4. **Filter** — In-memory filter by length/namespace from cached page_meta (default min 5000 bytes, excludes stubs)
5. **Fetch** — Multistream bz2 reader (default) or batch API fetch
6. **Extract** — Infobox parsing (primary) → NLP regex extraction (secondary, bio mode only) → Claude LLM fallback (tertiary) → date normalization
7. **Output** — Per-pattern-file Excel (default), or single CSV/TSV. Includes article_bytes, word_count, date note columns
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

- `--wiki NAME` — wiki project (default: enwiki; any language wiki supported, e.g., `frwiki`, `dewiki`, `simplewiki`)
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

### General Knowledge Patterns (`patterns/`)

| File | Patterns | Categories | Articles | Non-stub |
|---|---|---|---|---|
| biology.txt | 25 | 8,707 | 517,212 | 81,466 |
| astronomy.txt | 22 | 2,568 | 145,871 | 26,869 |
| battles_wars_conflicts.txt | 22 | 8,088 | 114,404 | 66,472 |
| mathematics_statistics.txt | 24 | 807 | 36,873 | 21,300 |
| explorations_voyages_spacecraft.txt | 18 | 1,473 | 18,305 | 11,461 |

Biographical patterns use word-boundary anchors (`(?:^|_)..(?:$|_)`). Geographic patterns use `(?:_in_|_of_)` suffixes matching Wikipedia's `Things_in_Place` convention. General knowledge patterns use a mix of both styles.

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
  cache.py           # pickle save/load with .meta mtime sidecar, wiki-namespaced
  dump_reader.py     # multistream index reader (batch I/O, regex plaintext), legacy fallback
  pipeline.py        # cache-aware orchestrator (regex + BFS modes)
  wiki_api.py        # MediaWiki API client (batched, rate-limited)
  infobox_base.py        # shared generic infobox extraction (InfoboxConfig + extract_infobox)
  infobox_parser.py      # biographical infobox config (person/artist/scientist/etc.)
  geo_infobox_parser.py  # geographic infobox config (settlement/city/county/etc.)
  extractors.py          # battle/exploration/astronomy/biology/math infobox configs + registry
  nlp_extractor.py       # regex extraction from first-sentence biographical patterns
  llm_extractor.py       # Claude Haiku fallback for missing fields
  output.py              # Excel/CSV/TSV writer (per-pattern-file xlsx default)

scripts/
  transform_to_gadm.py    # CSV → wikipedia.json keyed by GADM GID_2
  run_geo_integration.py   # orchestrator: pipeline → transform → map data dir

patterns/              # biographical + general knowledge regex pattern files
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

**Multistream index reader** (batch I/O + regex plaintext):
- Uses Wikimedia's multistream bz2 format: independently-compressed blocks with byte-offset index
- Index load: ~0.5s (551K pages); cached as pickle for instant reload
- Batches adjacent blocks into contiguous reads (97% of blocks within 2 MB of next)
- Plaintext conversion via fast regex strip (replaces mwparserfromhell AST parsing)
- enwiki (24 GB dump): ~50-60 articles/sec for 109K movie_directors_actors articles
- simplewiki (358 MB dump): ~176 articles/sec

**Full pipeline run** (2026-04-03, all 13 bio patterns → simplewiki):
- Cache load: ~31s; regex search (298 patterns): ~292s; multistream read: ~357s; extraction: ~330s
- Total: ~17 min for 69,126 articles (6.6% of 1,046,545 enwiki matches found in simplewiki)
- Fill rates: birth_date 70%, death_date 35%, nationality 68%, occupation 55%

**API fetch** (estimated from rate limits):
- ~10–20 articles/sec (50/batch, 0.1s rate limit, 2 passes for wikitext+plaintext)
- 1K articles: ~2 min; 35K: ~30–60 min; 1M: ~14–28 hrs
- Scales linearly with article count; efficient for small runs

**Disk usage** (data/):
- SQL dump caches: ~1.8 GB (catid_map, page_meta, lt_map, parsed_catlinks)
- Multistream dump (simplewiki): ~358 MB + ~5 MB index
- Multistream dump (enwiki): ~24 GB + ~267 MB index

## Tests

343 tests. Run: `.venv/bin/python -m pytest tests/ -v`

## Extraction Modes

All extractors share a generic `extract_infobox()` in `infobox_base.py` — each mode is just a config (infobox names + field aliases) and a one-line wrapper. `--extraction-mode auto` (default) selects the right extractor per pattern file.

### Biographical (`--extraction-mode bio`)

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

### Date normalization

Post-extraction `normalize_date` converts all date values to ISO `YYYY-MM-DD` (or `YYYY-MM`, `YYYY`, `YYYY BC`). Handles: MDY/DMY text, aged/age suffixes, HTML entities, ordinals, numeric DD-MM-YYYY / DD.MM.YYYY, BC/BCE, "or" alternatives (picks first), prefixes (On/Possibly/Died/Either), template junk (`}}`), date ranges, circa, incomplete ISO zero-padding.

Approximate dates get a sortable placeholder + note column: decades (`1900s` → `1900-01-01`, note `~1900s`), centuries (`19-century` → `1801-01-01`), month-day only (`January 10` → `9999-01-10`, note `~January 10 (no year)`).

**Known unhandled date edge cases** (pass through as-is):
- `Dead by 2 March 1942` — "dead by" prefix with extractable date
- `between 24 April 1563 and 23 April 1564` — date range with "between...and"
- `Uncertain; 11 August 1941?` — uncertainty prefix + question mark
- `18/19 December 1999` — day range within month
- `194 BC (Around 82)`, `348 BC (aged )` — BC year with parenthetical age
- `(aged around 75)` — age only, no date
- `February or March 1945` — month-level alternative

### Geographic (`--extraction-mode geo`)

Extracts population, area_km2, elevation_m, subdivision_name, subdivision_type from settlement/city/county/district/place infoboxes. Same three-tier cascade (geo infobox → NLP skipped → LLM fallback). Field aliases map common infobox variants (e.g., `population_total`, `pop` → `population`).

### Battle (`--extraction-mode battle`)

Extracts date, location, belligerents, result, casualties, commanders from `infobox military conflict`/`civilian attack`/`war faction`.

**Known issues:** belligerents/commanders fields contain concatenated names without consistent separators; wikitext list markers (`*`, `----`) leak into multi-value fields; result field can be very long (multi-sentence descriptions).

### Exploration (`--extraction-mode exploration`)

Extracts date, destination, origin, crew, mission_type, status from `infobox spaceflight`/`spacecraft class`/`space station`.

**Known issues:** low fill rates — destination 0% (nested sub-infoboxes stripped), status ~3%. Spaceflight infoboxes use deeply nested template structures not fully handled by the generic extractor.

### Astronomy (`--extraction-mode astronomy`)

Extracts type, distance, mass, radius, constellation, discovery_date, orbital_period, rotational_period from `infobox planet`/`star`/`galaxy`/`nebula`/`comet`/`asteroid`/`minor planet`/`pulsar`.

### Biology (`--extraction-mode biology`)

Extracts type, scientific_name, conservation_status, habitat, distribution from `speciesbox`/`taxobox`/`infobox disease`/`organism`.

**Known issues:** habitat 0% fill (field rarely populated in infoboxes); distribution returns map captions rather than text descriptions.

### Mathematics (`--extraction-mode math`)

Extracts field, year_discovered, discoverer, related_to from `infobox mathematical statement`/`theorem`/`conjecture`/`equation`.

**Known issues:** 0% fill across all fields — math articles on Wikipedia rarely use structured infoboxes; theorems/equations are described in prose only. LLM fallback would help here.

### Known field quality issues (all modes)

- **Bio occupation**: occasionally contains book titles or research descriptions instead of occupations
- **Bio nationality**: occasionally a country name instead of adjective (e.g., "Cameroon" vs "Cameroonian")
- **Battle multi-value fields**: names sometimes concatenated without separators; list markers leak through
- **All modes**: `_clean_value` strips nested templates up to 3 levels but deeply nested structures can leak

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

### Future: Dagster Workspace Orchestration

Goal: orchestrate data flow across a multi-product polyglot workspace via Dagster software-defined assets. Repos stay independent — no monorepo migration, no git subtree.

**Workspace scope (planned):**
- Products: wikipedia-data-analysis, global-geo-atlas, doc2md, productivity-guardian, email-review-documents
- ~8 data pipelines feeding global-geo-atlas: GADM, OSM (Geofabrik), HydroRIVERS, Natural Earth, RESOLVE Ecoregions, WorldPop, Wikipedia, plus future sources
- Loose coupling primarily via data exchange; potential shared library code later

**Why Dagster (not Bazel/Nx/Moon/Make/custom orchestrator):**
- Software-defined assets model fits per-country data files exactly — each `wikipedia.json`, `worldpop.json`, etc. is one asset, with declared upstream inputs
- **Code locations** let each repo register its assets independently — no unified codebase, no history merge, repos remain publishable on their own
- **Per-country partitions** give `--country {id}` and `--all` essentially for free, with parallelism + retries + per-partition status
- **Caching**: skip re-extraction when upstream Wikipedia dump (or OSM PBF, GADM release) is unchanged
- **Lineage UI** shows freshness across ~8 pipelines × 199 countries (~1,600 logical units)
- global-geo-atlas build itself becomes a downstream asset depending on all data assets — one command rebuilds only what's stale
- Published, accepted pattern (Mapbox, Drizly, Prezi, ShopRunner); the asset-oriented model is the modern alternative to Airflow's task-oriented model

**Migration path (incremental, reversible):**

1. **Workspace skeleton first** — top-level `~/Documents/workspace/` repo with `mise.toml` (shared Python/Node versions), `Taskfile.yml` (cross-repo tasks: `task atlas:build`, `task wiki:extract`), `repos.yaml` (sibling-checkout manifest). One afternoon, pure upside, no lock-in.
2. **Wikipedia pipeline as first Dagster code location** — partition assets by country id; upstream = `enwiki` dump asset (mtime-tracked); downstream = `wikipedia.json` written into `global-geo-atlas/data/{id}/`
3. **Add code locations per repo as data flow grows** — each sibling repo registers its assets without giving up independence
4. **Cross-repo edges** — e.g., `doc2md` output → `email-review-documents` input modeled as plain asset dependencies
5. **Defer true monorepo migration** (Moon/Pants/Bazel) until shared *code* (not data) becomes painful. Not before.

**Rejected alternative — git subtree + custom country-loop orchestrator:** acceptable at 2 projects; at 5–8 it reinvents Dagster (parameterization, retries, caching, dependency graph, observability) badly. Custom Python orchestrator becomes its own maintenance burden.

**Why not Nx/Turborepo:** JS/TS-first; Python and shell pipelines are second-class via custom executors. Wrong abstraction for data-pipeline-heavy polyglot workspace.

**Why not Bazel/Pants/Moon yet:** designed for code that compiles and links. Pipelines here are fetch-from-internet → GDAL/osmium → write JSON; build-system hermeticity rules fight non-hermetic data sources. Revisit only if shared library code becomes substantial.

## TODO

- **Re-run simplewiki bio extraction** — `min_page_length` now defaults to 0 for simplewiki (was 5000), recovering ~65k additional articles including short stubs. Re-run `uv run python -m wiki_pipeline --wiki simplewiki` to regenerate Excel files in `results/simplewiki/`. Caches are already built; bulk of time is multistream read + extraction for new articles. Motivated by cross-repo entity matching with doc2md index entries (Cambridge History of Science v2 gains ~38 matches from the lower threshold).

## Key Design Decisions

- **Regex as default search** — scan all category names with user-defined patterns; more flexible than single-root BFS for thematic queries
- **BFS retained as option** — useful when category hierarchy matters or a clean root exists
- **Cache is search-independent** — dump parsing is global; only search (regex/BFS) depends on user input
- **Single-pass page dump** — produces both catid_map (ns=14) and page_meta (ns=0, non-redirect) in one pass
- **Mtime sidecar validation** — `.meta` files track source dump mtimes; stale cache auto-invalidated without loading full pickle
- **Streaming SQL parser** — `str.find()` scanning for multi-hundred-MB INSERT lines, O(1) memory per row
- **Multistream index reader** — seeks directly into compressed bz2 blocks via byte-offset index; reads only target pages instead of scanning entire dump. Default over legacy full-scan.
- **Wiki-language-agnostic** — all URLs, caches, and output parameterized by `--wiki`; `wiki_to_lang()` maps wiki name to language code for API/URL generation; caches namespaced under `data/.cache/{wiki}/`; NLP/infobox extractors degrade gracefully for non-English wikis (fall through to LLM)
- **Three-tier extraction** — infobox → NLP regex → LLM fallback → None; NLP tier eliminates most LLM calls at zero cost
- **Word-boundary anchored patterns** — prevent false positives from substring matches (e.g., `count` in "county")
- **Dual extraction modes** — bio/geo share pipeline infrastructure; only infobox parser and default fields differ
- **Per-pattern-file Excel output** — default xlsx with one file per pattern file; includes article_bytes, word_count, date note columns
- **Post-extraction date normalization** — `normalize_date_with_note` converts all date formats to ISO with sortable placeholders for approximate dates (decades, centuries, month-only)
- **Batch multistream I/O** — adjacent bz2 blocks read in contiguous batches; regex-based plaintext conversion replaces mwparserfromhell AST parsing in dump reader
- **GADM join by title normalization** — avoids external dependency on Wikidata; suffix stripping handles County/Parish/Borough variants
