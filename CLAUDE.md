# CLAUDE.md — wikipedia-data-analysis

Wikipedia category extraction pipeline. Regex or BFS search over ~2.6M category names → infobox parsing → biographical or geographic structured output. Integrates with global-geo-atlas via `transform` stage.

See [PRD.md](PRD.md) for full pipeline stages, pattern files, extraction modes, and CLI flags.

## Architecture

```
src/wiki_pipeline/
├── pipeline.py       # CLI entry point; PipelineConfig; run()
├── transform.py      # CSV → wikipedia.json keyed by GADM GID_2 (used by Dagster)
├── cache.py          # mtime-validated pickle cache (~1.8 GB for enwiki)
├── search.py         # regex + BFS category search
├── fetch.py          # multistream bz2 reader (default) or batch API
├── extract/          # infobox parser → NLP regex → Ollama LLM fallback
└── output.py         # Excel / CSV / TSV writer
```

## Pipeline stages

1. Download SQL dumps (page, categorylinks, linktarget — ~5.7 GB compressed)
2. Parse & cache → `data/.cache/{wiki}/` (single-pass, mtime-validated)
3. Search — regex on category names or BFS from root
4. Filter — min 5000 bytes, excludes stubs
5. Fetch — multistream bz2 (preferred) or API
6. Extract — infobox → NLP regex → LLM fallback
7. Output — Excel (default), CSV, TSV
8. Transform (optional) — `transform.py` → `wikipedia.json` for atlas

## Dagster integration

`transform.py` is imported directly by the orchestrator (`from wiki_pipeline.transform import transform`). Changes here require `dagster dev` restart in `personal-project-orchestrator/` (editable install, no hot reload).

## LLM config

Ollama model and endpoint are read from `OLLAMA_MODEL` / `OLLAMA_BASE_URL` env vars (canonical values in `~/Documents/Projects/.env`). Override per-run via CLI flags `--ollama-model` / `--ollama-base-url`, or set `PPO_SKIP_LLM=1` in the orchestrator to skip LLM entirely.

## Key facts

- Default wiki: `enwiki` — simplewiki lacks structured infobox templates (0 matches for any country)
- Cache pickles: 4 × ~1.7 GB total; must exist before smoke runs
- Full enwiki dump: ~22 GB compressed (not committed)
- Pattern files in `patterns/` cover 13 biographical categories + 5 geo categories
