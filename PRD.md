# Wikipedia Category Data Pipeline

Extract structured data (birth/death dates, nationality, occupation) from Wikipedia articles found via regex category search or BFS traversal.

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
6. **Extract** — Infobox parsing (primary) → NLP regex extraction (secondary) → Claude LLM fallback (tertiary)
7. **Output** — CSV/TSV with page_id, title, and extracted fields

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
```

### Flags

- `--patterns REGEX [REGEX ...]` — regex patterns to match category names (case-insensitive)
- `--patterns-file PATH` — file with one regex pattern per line (combinable with `--patterns`)
- `--max-depth N` — limit BFS to N levels below root (default: unlimited)
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

Patterns use word-boundary anchors (`(?:^|_)..(?:$|_)`) to avoid false positives. Broad terms like `artist` or `king` are either qualified (`graphic_artists`) or bounded to exclude noise (place names, band names, etc.).

## Architecture

```
src/wiki_pipeline/
  config.py          # frozen dataclass + argparse CLI, search_mode property
  download.py        # streaming download w/ resume + exponential backoff
  sql_parser.py      # streaming MySQL INSERT parser (C-speed str.find)
  category_tree.py   # parse_page_dump, parse_category_links, bfs_from_root,
                     # find_categories_by_regex, collect_articles_from_categories
  page_filter.py     # filter_pages (streaming) + filter_pages_from_meta (cached)
  cache.py           # pickle save/load with .meta mtime sidecar
  pipeline.py        # cache-aware orchestrator (regex + BFS modes)
  wiki_api.py        # MediaWiki API client (batched, rate-limited)
  infobox_parser.py  # mwparserfromhell field extraction
  nlp_extractor.py   # regex extraction from first-sentence biographical patterns
  llm_extractor.py   # Claude Haiku fallback for missing fields
  output.py          # CSV/TSV writer

patterns/             # pre-built regex pattern files (one pattern per line)
results/              # output directory for pipeline results
```

## Performance

With cached dumps (~21s load time):
- Regex search: ~21s for 23 patterns × 2.6M category names
- Article collection + filtering: <0.1s
- Total cached run: ~42s regardless of pattern count

Initial parse (cold cache): ~30 min (one-time cost, shared across all searches)

## Tests

187 tests. Run: `.venv/bin/python -m pytest tests/ -v`

## Extraction Tiers

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

## Key Design Decisions

- **Regex as default search** — scan all category names with user-defined patterns; more flexible than single-root BFS for thematic queries
- **BFS retained as option** — useful when category hierarchy matters or a clean root exists
- **Cache is search-independent** — dump parsing is global; only search (regex/BFS) depends on user input
- **Single-pass page dump** — produces both catid_map (ns=14) and page_meta (ns=0, non-redirect) in one pass
- **Mtime sidecar validation** — `.meta` files track source dump mtimes; stale cache auto-invalidated without loading full pickle
- **Streaming SQL parser** — `str.find()` scanning for multi-hundred-MB INSERT lines, O(1) memory per row
- **Three-tier extraction** — infobox → NLP regex → LLM fallback → None; NLP tier eliminates most LLM calls at zero cost
- **Word-boundary anchored patterns** — prevent false positives from substring matches (e.g., `count` in "county")
