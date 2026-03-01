"""Pipeline configuration via frozen dataclass + argparse CLI."""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class PipelineConfig:
    root_category: str
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
    output_format: str = "csv"
    dry_run: bool = False
    no_cache: bool = False
    clear_cache: bool = False


def parse_args(argv: list[str] | None = None) -> PipelineConfig:
    """Parse CLI arguments into PipelineConfig."""
    p = argparse.ArgumentParser(description="Wikipedia category data pipeline")
    p.add_argument("root_category", help="Root Wikipedia category name (without Category: prefix)")
    p.add_argument("--min-page-length", type=int, default=5000)
    p.add_argument("--data-dir", type=Path, default=Path("data"))
    p.add_argument("--results-dir", type=Path, default=Path("results/pipeline"))
    p.add_argument("--api-batch-size", type=int, default=50)
    p.add_argument("--api-rate-limit", type=float, default=0.1)
    p.add_argument("--claude-model", default="claude-haiku-4-5-20251001")
    p.add_argument("--required-fields", nargs="+", default=["birth_date", "death_date", "nationality", "occupation"])
    p.add_argument("--output-format", choices=["csv", "tsv"], default="csv")
    p.add_argument("--dry-run", action="store_true", help="Stop before API calls, report counts")
    p.add_argument("--no-cache", action="store_true", help="Bypass cache read/write")
    p.add_argument("--clear-cache", action="store_true", help="Clear cache before run")
    args = p.parse_args(argv)
    return PipelineConfig(
        root_category=args.root_category,
        min_page_length=args.min_page_length,
        data_dir=args.data_dir,
        results_dir=args.results_dir,
        api_batch_size=args.api_batch_size,
        api_rate_limit_s=args.api_rate_limit,
        claude_model=args.claude_model,
        required_fields=tuple(args.required_fields),
        output_format=args.output_format,
        dry_run=args.dry_run,
        no_cache=args.no_cache,
        clear_cache=args.clear_cache,
    )
