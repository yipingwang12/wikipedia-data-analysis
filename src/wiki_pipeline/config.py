"""Pipeline configuration via frozen dataclass + argparse CLI."""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class PipelineConfig:
    root_category: str | None = None
    category_patterns: tuple[str, ...] = ()
    min_page_length: int = 5000
    data_dir: Path = Path("data")
    results_dir: Path = Path("results/pipeline")
    dump_base_url: str = "https://dumps.wikimedia.org/enwiki/latest"
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
    args = p.parse_args(argv)

    # Collect patterns from --patterns and/or --patterns-file
    patterns: list[str] = []
    if args.patterns:
        patterns.extend(args.patterns)
    if args.patterns_file:
        text = args.patterns_file.read_text()
        patterns.extend(line.strip() for line in text.splitlines() if line.strip())

    if not args.root_category and not patterns:
        p.error("provide either root_category (BFS mode) or --patterns/--patterns-file (regex mode)")

    # Default required_fields depends on extraction mode
    if args.required_fields is not None:
        required_fields = tuple(args.required_fields)
    elif args.extraction_mode == "geo":
        required_fields = ("population", "area_km2", "elevation_m", "subdivision_name", "subdivision_type")
    else:
        required_fields = ("birth_date", "death_date", "nationality", "occupation")

    return PipelineConfig(
        root_category=args.root_category,
        category_patterns=tuple(patterns),
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
    )
