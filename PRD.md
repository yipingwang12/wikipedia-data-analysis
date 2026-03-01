# Wikipedia Category Data Pipeline

Extract structured data (birth/death dates, nationality, occupation) from Wikipedia articles within any category tree.

## Pipeline Stages

1. **Download** — 3 SQL dumps from Wikimedia (~5.7 GB compressed): `page`, `categorylinks`, `linktarget`
2. **Parse & Cache** — Single-pass page dump (catid_map + page_meta), linktarget map, full category adjacency. Cached as pickle with mtime-validated sidecars in `data/.cache/` (~1.8 GB)
3. **BFS** — Traverse cached adjacency from any `root_category` to collect article IDs (milliseconds)
4. **Filter** — In-memory filter by length/namespace from cached page_meta
5. **Fetch** — Batch wikitext + plaintext via MediaWiki API
6. **Extract** — Infobox parsing (primary) → Claude LLM fallback (secondary)
7. **Output** — CSV/TSV with page_id, title, and extracted fields

## Usage

```bash
wiki-pipeline French_painters --dry-run          # first run: parse + cache (~30 min)
wiki-pipeline Painters_by_nationality --dry-run   # cached run: ~10s
wiki-pipeline French_painters                     # full run: fetch + extract + write
```

### Cache flags

- `--no-cache` — bypass cache read/write, force full re-parse
- `--clear-cache` — delete cached files before run

## Architecture

```
src/wiki_pipeline/
  config.py          # frozen dataclass + argparse CLI
  download.py        # streaming download w/ resume + exponential backoff
  sql_parser.py      # streaming MySQL INSERT parser (C-speed str.find)
  category_tree.py   # parse_page_dump, parse_category_links, bfs_from_root
  page_filter.py     # filter_pages (streaming) + filter_pages_from_meta (cached)
  cache.py           # pickle save/load with .meta mtime sidecar
  pipeline.py        # cache-aware orchestrator
  wiki_api.py        # MediaWiki API client (batched, rate-limited)
  infobox_parser.py  # mwparserfromhell field extraction
  llm_extractor.py   # Claude Haiku fallback for missing fields
  output.py          # CSV/TSV writer
```

## Tests

113 tests, 96% coverage. Run: `.venv/bin/python -m pytest tests/ -v`

## Key Design Decisions

- **Cache is category-independent** — dump parsing is global; only BFS depends on root_category
- **Single-pass page dump** — produces both catid_map (ns=14) and page_meta (ns=0, non-redirect) in one pass
- **Mtime sidecar validation** — `.meta` files track source dump mtimes; stale cache auto-invalidated without loading full pickle
- **Streaming SQL parser** — `str.find()` scanning for multi-hundred-MB INSERT lines, O(1) memory per row
- **Graceful degradation** — infobox → LLM fallback → None for missing fields
