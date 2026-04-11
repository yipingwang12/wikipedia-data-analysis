"""CLI entry point for wiki_pipeline.transform.

Kept as a script for backwards compatibility with run_geo_integration.py
and direct command-line invocation. Business logic lives in
src/wiki_pipeline/transform.py.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from wiki_pipeline.transform import transform


def main() -> None:
    p = argparse.ArgumentParser(description="Transform pipeline CSV to GADM wikipedia.json")
    p.add_argument("--csv", type=Path, required=True, help="Pipeline CSV output")
    p.add_argument("--gadm-data-dir", type=Path, required=True, help="GADM country data dir (contains regions/)")
    p.add_argument("--output", type=Path, required=True, help="Output wikipedia.json path")
    p.add_argument("--wiki", default="enwiki", help="Wiki project name (e.g., enwiki, frwiki)")
    args = p.parse_args()
    transform(args.csv, args.gadm_data_dir, args.output, wiki=args.wiki)


if __name__ == "__main__":
    main()
