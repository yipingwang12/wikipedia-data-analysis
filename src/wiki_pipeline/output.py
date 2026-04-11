"""CSV/TSV/Excel writer for pipeline results."""

from __future__ import annotations

import csv
from pathlib import Path

import openpyxl


def write_results(
    records: list[dict[str, str | int | None]],
    output_path: Path,
    fmt: str = "xlsx",
) -> Path:
    """Write records to CSV, TSV, or Excel. None values become empty strings."""
    if fmt == "xlsx":
        return write_excel(records, output_path)
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


def write_excel(
    records: list[dict[str, str | int | None]],
    output_path: Path,
    sheet_name: str | None = None,
) -> Path:
    """Write records to an Excel .xlsx file with auto-sized columns."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    wb = openpyxl.Workbook()
    ws = wb.active
    if sheet_name:
        ws.title = sheet_name

    if not records:
        wb.save(output_path)
        return output_path

    fieldnames = list(records[0].keys())
    ws.append(fieldnames)
    for rec in records:
        ws.append([rec.get(h) for h in fieldnames])

    for col in ws.columns:
        max_len = max(len(str(cell.value or "")) for cell in col)
        ws.column_dimensions[col[0].column_letter].width = min(max_len + 2, 50)

    wb.save(output_path)
    return output_path
