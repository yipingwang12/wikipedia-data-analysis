"""Pipeline configuration via frozen dataclass + argparse CLI."""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from pathlib import Path

BIO_PATTERNS_DIR = Path("patterns")
GEO_PATTERNS_DIR = Path("patterns/geo")


@dataclass(frozen=True)
class PipelineConfig:
    wiki: str = "simplewiki"
    root_category: str | None = None
    category_patterns: tuple[str, ...] = ()
    min_page_length: int = 5000
    data_dir: Path = Path("data")
    results_dir: Path = Path("results/pipeline")
    dump_base_url: str = "https://dumps.wikimedia.org/simplewiki/latest"
    wiki_api_url: str = "https://en.wikipedia.org/w/api.php"
    api_batch_size: int = 50
    api_rate_limit_s: float = 0.1
    claude_model: str = "claude-haiku-4-5-20251001"
    required_fields: tuple[str, ...] = (
        "birth_date",
        "death_date",
        "nationality",
        "occupation",
    )
    extraction_mode: str = "bio"
    output_format: str = "csv"
    max_depth: int | None = None
    dry_run: bool = False
    no_cache: bool = False
    clear_cache: bool = False
    download_articles: bool = False
    use_api: bool = False
    use_multistream: bool = True
    limit: int | None = None

    @property
    def search_mode(self) -> str:
        """Return 'regex' if patterns provided, else 'bfs'."""
        return "regex" if self.category_patterns else "bfs"


def parse_args(argv: list[str] | None = None) -> PipelineConfig:
    """Parse CLI arguments into PipelineConfig."""
    p = argparse.ArgumentParser(description="Wikipedia category data pipeline")
    p.add_argument("root_category", nargs="?", default=None,
                    help="Root Wikipedia category name for BFS mode (without Category: prefix)")
    p.add_argument("--patterns", nargs="+", default=None,
                    help="Regex patterns to match category names (default search mode)")
    p.add_argument("--patterns-file", type=Path, default=None,
                    help="File with one regex pattern per line")
    p.add_argument("--min-page-length", type=int, default=5000)
    p.add_argument("--wiki", default="enwiki",
                    help="Wiki project name (e.g., enwiki, simplewiki)")
    p.add_argument("--data-dir", type=Path, default=Path("data"))
    p.add_argument("--results-dir", type=Path, default=Path("results/pipeline"))
    p.add_argument("--api-batch-size", type=int, default=50)
    p.add_argument("--api-rate-limit", type=float, default=0.1)
    p.add_argument("--claude-model", default="claude-haiku-4-5-20251001")
    p.add_argument("--extraction-mode", choices=["bio", "geo"], default="bio",
                    help="Extraction mode: bio (biographical) or geo (geographic)")
    p.add_argument("--required-fields", nargs="+", default=None,
                    help="Fields to extract (default depends on extraction mode)")
    p.add_argument("--output-format", choices=["csv", "tsv"], default="csv")
    p.add_argument("--max-depth", type=int, default=None, help="Max BFS depth (0=root only, None=unlimited)")
    p.add_argument("--dry-run", action="store_true", help="Stop before API calls, report counts")
    p.add_argument("--no-cache", action="store_true", help="Bypass cache read/write")
    p.add_argument("--clear-cache", action="store_true", help="Clear cache before run")
    p.add_argument("--download-articles", action="store_true",
                    help="Download full article content dump (enwiki-latest-pages-articles.xml.bz2, ~22 GB)")
    p.add_argument("--use-api", action="store_true",
                    help="Fetch article content via MediaWiki API instead of local dump (default: read from dump)")
    p.add_argument("--no-multistream", action="store_true",
                    help="Use legacy full-scan dump reader instead of multistream index (default: multistream)")
    p.add_argument("--limit", type=int, default=None,
                    help="Stop after extracting N articles (useful for testing)")
    p.add_argument("--all", action="store_true",
                    help="Load all pattern files for the extraction mode (default when using dump reader in bio mode)")
    args = p.parse_args(argv)

    # Collect patterns from --patterns and/or --patterns-file
    patterns: list[str] = []
    if args.patterns:
        patterns.extend(args.patterns)
    if args.patterns_file:
        text = args.patterns_file.read_text()
        patterns.extend(line.strip() for line in text.splitlines() if line.strip())

    # Default to --all when using dump reader in bio mode with no explicit patterns
    use_all = args.all or (not args.use_api and not patterns and not args.root_category
                           and not args.download_articles)
    if use_all and not patterns:
        patterns_dir = GEO_PATTERNS_DIR if args.extraction_mode == "geo" else BIO_PATTERNS_DIR
        for pat_file in sorted(patterns_dir.glob("*.txt")):
            text = pat_file.read_text()
            patterns.extend(line.strip() for line in text.splitlines() if line.strip())
        if patterns:
            print(f"Loaded {len(patterns)} patterns from {patterns_dir}", flush=True)

    if not args.root_category and not patterns and not args.download_articles:
        p.error("provide either root_category (BFS mode), --patterns/--patterns-file (regex mode), or --download-articles")

    # Default required_fields depends on extraction mode
    if args.required_fields is not None:
        required_fields = tuple(args.required_fields)
    elif args.extraction_mode == "geo":
        required_fields = ("population", "area_km2", "elevation_m", "subdivision_name", "subdivision_type")
    else:
        required_fields = ("birth_date", "death_date", "nationality", "occupation")

    wiki = args.wiki
    dump_base_url = f"https://dumps.wikimedia.org/{wiki}/latest"

    return PipelineConfig(
        wiki=wiki,
        root_category=args.root_category,
        category_patterns=tuple(patterns),
        dump_base_url=dump_base_url,
        min_page_length=args.min_page_length,
        data_dir=args.data_dir,
        results_dir=args.results_dir,
        api_batch_size=args.api_batch_size,
        api_rate_limit_s=args.api_rate_limit,
        claude_model=args.claude_model,
        required_fields=required_fields,
        extraction_mode=args.extraction_mode,
        output_format=args.output_format,
        max_depth=args.max_depth,
        dry_run=args.dry_run,
        no_cache=args.no_cache,
        clear_cache=args.clear_cache,
        download_articles=args.download_articles,
        use_api=args.use_api,
        use_multistream=not args.no_multistream,
        limit=args.limit,
    )
