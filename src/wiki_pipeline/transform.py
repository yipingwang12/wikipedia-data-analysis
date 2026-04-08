"""Transform pipeline CSV output to wikipedia.json keyed by GADM GID_2."""

from __future__ import annotations

import csv
import json
import re
from pathlib import Path

_SUFFIX_RE = re.compile(
    r"\s+(?:county|parish|borough|census\s+area|municipality|district"
    r"|department|prefecture|province|commune)$",
    re.IGNORECASE,
)


def normalize(title: str) -> str:
    """Lowercase, replace underscores, strip admin suffixes, collapse whitespace."""
    s = title.replace("_", " ")
    parts = [p.strip() for p in s.split(",", 1)]
    parts[0] = _SUFFIX_RE.sub("", parts[0])
    s = ", ".join(parts)
    return re.sub(r"\s+", " ", s).strip().lower()


def build_gadm_index(gadm_data_dir: Path) -> dict[str, str]:
    """Walk regions/*.json, return {normalized_key: GID_2}.

    Key = normalize(NAME_2 + ", " + NAME_1).
    """
    index: dict[str, str] = {}
    regions_dir = gadm_data_dir / "regions"
    if not regions_dir.is_dir():
        return index

    for path in sorted(regions_dir.glob("*.json")):
        with open(path) as f:
            data = json.load(f)
        for feature in data.get("features", []):
            props = feature.get("properties", {})
            gid2 = props.get("GID_2")
            name2 = props.get("NAME_2")
            name1 = props.get("NAME_1")
            if not gid2 or not name2 or not name1:
                continue
            key = normalize(f"{name2}, {name1}")
            index[key] = gid2
    return index


def transform(csv_path: Path, gadm_data_dir: Path, output_path: Path) -> None:
    """Read CSV, match titles to GID_2, write wikipedia.json."""
    index = build_gadm_index(gadm_data_dir)
    result: dict[str, dict] = {}
    unmatched: list[dict] = []

    with open(csv_path) as f:
        reader = csv.DictReader(f)
        fields = [c for c in (reader.fieldnames or []) if c not in ("page_id", "title")]

        for row in reader:
            title = row.get("title", "")
            key = normalize(title)
            gid2 = index.get(key)

            if gid2 is None:
                unmatched.append(row)
                continue

            entry: dict[str, str] = {"title": title}
            for field in fields:
                val = row.get(field)
                if val:
                    entry[field] = val
            entry["wikipedia_url"] = (
                "https://en.wikipedia.org/wiki/" + title.replace(" ", "_")
            )
            result[gid2] = entry

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(result, f, indent=2)

    if unmatched:
        unmatched_path = output_path.parent / "unmatched.csv"
        with open(unmatched_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=unmatched[0].keys())
            w.writeheader()
            w.writerows(unmatched)
        print(f"  {len(unmatched)} unmatched rows → {unmatched_path}")

    print(f"  {len(result)} matched → {output_path}")
