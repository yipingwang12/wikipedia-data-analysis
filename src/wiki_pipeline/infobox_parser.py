"""Infobox field extraction using mwparserfromhell."""

from __future__ import annotations

import re

import mwparserfromhell

PERSON_INFOBOX_NAMES = {
    "infobox person",
    "infobox artist",
    "infobox painter",
    "infobox musical artist",
    "infobox writer",
    "infobox scientist",
    "infobox politician",
    "infobox philosopher",
    "infobox military person",
    "infobox sportsperson",
    "infobox actor",
    "infobox architect",
    "infobox composer",
}

FIELD_ALIASES: dict[str, list[str]] = {
    "birth_date": ["birth_date", "born", "birthdate"],
    "death_date": ["death_date", "died", "deathdate"],
    "nationality": ["nationality", "citizenship", "country"],
    "occupation": ["occupation", "known_for", "notable_works"],
    "birth_place": ["birth_place", "birthplace", "born"],
    "death_place": ["death_place", "deathplace"],
}


def extract_infobox_fields(
    wikitext: str, required_fields: tuple[str, ...] | list[str]
) -> dict[str, str | None]:
    """Parse wikitext, extract fields from person-type infoboxes."""
    result: dict[str, str | None] = {f: None for f in required_fields}
    parsed = mwparserfromhell.parse(wikitext)
    templates = parsed.filter_templates()

    infobox = None
    for t in templates:
        name = t.name.strip().lower()
        if name in PERSON_INFOBOX_NAMES:
            infobox = t
            break

    if infobox is None:
        return result

    for field_name in required_fields:
        aliases = FIELD_ALIASES.get(field_name, [field_name])
        for alias in aliases:
            if infobox.has(alias):
                raw = str(infobox.get(alias).value).strip()
                cleaned = _clean_value(raw, field_name)
                if cleaned:
                    result[field_name] = cleaned
                    break

    return result


def _clean_value(raw: str, field_name: str) -> str | None:
    """Clean a raw infobox value: resolve date templates, strip markup."""
    if not raw:
        return None

    # Resolve {{birth date|Y|M|D}} / {{death date|Y|M|D}} templates
    if field_name in ("birth_date", "death_date"):
        date = _resolve_date_template(raw)
        if date:
            return date

    # Strip wikilinks: [[link|text]] -> text, [[link]] -> link
    cleaned = re.sub(r"\[\[(?:[^|\]]*\|)?([^\]]*)\]\]", r"\1", raw)
    # Strip refs
    cleaned = re.sub(r"<ref[^>]*>.*?</ref>", "", cleaned, flags=re.DOTALL)
    cleaned = re.sub(r"<ref[^/]*/?>", "", cleaned)
    # Strip remaining HTML tags
    cleaned = re.sub(r"<[^>]+>", "", cleaned)
    # Strip remaining templates (simple)
    cleaned = re.sub(r"\{\{[^}]*\}\}", "", cleaned)
    cleaned = cleaned.strip().strip(",").strip()
    return cleaned if cleaned else None


def _resolve_date_template(raw: str) -> str | None:
    """Extract ISO date from {{birth date|Y|M|D}} style templates."""
    m = re.search(
        r"\{\{\s*(?:birth date|death date|birth date and age|death date and age)"
        r"(?:\s*\|[^|]*)*?"
        r"\|\s*(\d{4})\s*\|\s*(\d{1,2})\s*\|\s*(\d{1,2})",
        raw,
        re.IGNORECASE,
    )
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    # Try {{birth date|Y|M|D|...}} with mf=y or df=y flags before numbers
    m = re.search(
        r"\{\{\s*(?:birth date|death date|birth date and age|death date and age)"
        r"\s*\|.*?(\d{4})\s*\|\s*(\d{1,2})\s*\|\s*(\d{1,2})",
        raw,
        re.IGNORECASE,
    )
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    return None
