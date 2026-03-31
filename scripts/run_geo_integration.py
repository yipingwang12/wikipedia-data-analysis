"""Orchestrate: run Wikipedia pipeline with geo patterns → transform → place in map data dir."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
DEFAULT_MAP_DATA = Path.home() / "Documents" / "Projects" / "global-geo-atlas" / "data"
GEO_PATTERNS_DIR = PROJECT_DIR / "patterns" / "geo"


def run_pipeline(
    patterns_file: Path,
    results_dir: Path,
    extraction_mode: str = "geo",
    dry_run: bool = False,
) -> Path | None:
    """Run the Wikipedia pipeline and return the CSV output path."""
    cmd = [
        sys.executable, "-m", "wiki_pipeline.pipeline",
        "--patterns-file", str(patterns_file),
        "--extraction-mode", extraction_mode,
        "--results-dir", str(results_dir),
    ]
    if dry_run:
        cmd.append("--dry-run")

    print(f"Running pipeline: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=str(PROJECT_DIR))
    if result.returncode != 0:
        print(f"Pipeline failed with exit code {result.returncode}", file=sys.stderr)
        return None

    if dry_run:
        return None

    csvs = sorted(results_dir.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    return csvs[0] if csvs else None


def run_transform(csv_path: Path, gadm_data_dir: Path, output_path: Path) -> None:
    """Run the GADM transform script."""
    cmd = [
        sys.executable, str(SCRIPT_DIR / "transform_to_gadm.py"),
        "--csv", str(csv_path),
        "--gadm-data-dir", str(gadm_data_dir),
        "--output", str(output_path),
    ]
    print(f"Running transform: {' '.join(cmd)}")
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print(f"Transform failed with exit code {result.returncode}", file=sys.stderr)
        sys.exit(1)


def main() -> None:
    p = argparse.ArgumentParser(description="Run geo pipeline + transform for a country")
    p.add_argument("--country", required=True, help="Country data directory name (e.g., us, gb)")
    p.add_argument("--map-data-dir", type=Path, default=DEFAULT_MAP_DATA,
                    help="Path to global-geo-atlas/data/")
    p.add_argument("--patterns-file", type=Path, default=GEO_PATTERNS_DIR / "admin_subdivisions.txt",
                    help="Geo pattern file to use")
    p.add_argument("--dry-run", action="store_true", help="Pipeline dry-run only (no API calls)")
    args = p.parse_args()

    gadm_data_dir = args.map_data_dir / args.country
    if not (gadm_data_dir / "regions").is_dir():
        print(f"No regions/ directory found at {gadm_data_dir}", file=sys.stderr)
        sys.exit(1)

    results_dir = PROJECT_DIR / "results" / "geo"
    results_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: Run pipeline
    csv_path = run_pipeline(args.patterns_file, results_dir, dry_run=args.dry_run)
    if args.dry_run:
        print("Dry run complete.")
        return
    if csv_path is None:
        print("No CSV output found.", file=sys.stderr)
        sys.exit(1)

    # Step 2: Transform
    output_path = gadm_data_dir / "wikipedia.json"
    run_transform(csv_path, gadm_data_dir, output_path)

    print(f"Done: {output_path}")


if __name__ == "__main__":
    main()
