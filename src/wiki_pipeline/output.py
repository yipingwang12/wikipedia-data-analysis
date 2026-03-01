"""CSV/TSV writer for pipeline results."""

from __future__ import annotations

import csv
from pathlib import Path


def write_results(
    records: list[dict[str, str | int | None]],
    output_path: Path,
    fmt: str = "csv",
) -> Path:
    """Write records to CSV or TSV. None values become empty strings."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    delimiter = "\t" if fmt == "tsv" else ","

    if not records:
        output_path.write_text("")
        return output_path

    fieldnames = list(records[0].keys())
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=delimiter, extrasaction="ignore")
        writer.writeheader()
        for rec in records:
            cleaned = {k: ("" if v is None else v) for k, v in rec.items()}
            writer.writerow(cleaned)

    return output_path
