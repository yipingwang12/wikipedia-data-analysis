"""Geographic infobox field extraction using mwparserfromhell."""

from __future__ import annotations

import re

import mwparserfromhell

GEO_INFOBOX_NAMES = {
    "infobox settlement",
    "infobox city",
    "infobox municipality",
    "infobox county",
    "infobox district",
    "infobox place",
}

GEO_FIELD_ALIASES: dict[str, list[str]] = {
    "population": ["population_total", "pop", "population"],
    "area_km2": ["area_km2", "area_total_km2", "area"],
    "elevation_m": ["elevation_m", "elevation"],
    "subdivision_name": ["subdivision_name1", "state", "region", "province"],
    "subdivision_type": ["subdivision_type1"],
}


def extract_geo_infobox_fields(
    wikitext: str, required_fields: tuple[str, ...] | list[str]
) -> dict[str, str | None]:
    """Parse wikitext, extract fields from geographic infoboxes."""
    result: dict[str, str | None] = {f: None for f in required_fields}
    parsed = mwparserfromhell.parse(wikitext)
    templates = parsed.filter_templates()

    infobox = None
    for t in templates:
        name = t.name.strip().lower()
        if name in GEO_INFOBOX_NAMES:
            infobox = t
            break

    if infobox is None:
        return result

    for field_name in required_fields:
        aliases = GEO_FIELD_ALIASES.get(field_name, [field_name])
        for alias in aliases:
            if infobox.has(alias):
                raw = str(infobox.get(alias).value).strip()
                cleaned = _clean_value(raw, field_name)
                if cleaned:
                    result[field_name] = cleaned
                    break

    return result


def _clean_value(raw: str, field_name: str) -> str | None:
    """Clean a raw infobox value: strip markup, normalize numeric fields."""
    if not raw:
        return None

    # Strip wikilinks: [[link|text]] -> text, [[link]] -> link
    cleaned = re.sub(r"\[\[(?:[^|\]]*\|)?([^\]]*)\]\]", r"\1", raw)
    # Strip refs
    cleaned = re.sub(r"<ref[^>]*>.*?</ref>", "", cleaned, flags=re.DOTALL)
    cleaned = re.sub(r"<ref[^/]*/?>", "", cleaned)
    # Strip remaining HTML tags
    cleaned = re.sub(r"<[^>]+>", "", cleaned)
    # Strip remaining templates
    cleaned = re.sub(r"\{\{[^}]*\}\}", "", cleaned)
    cleaned = cleaned.strip().strip(",").strip()

    if not cleaned:
        return None

    # Strip commas from numeric fields
    if field_name in ("population", "area_km2", "elevation_m"):
        cleaned = cleaned.replace(",", "")

    return cleaned
