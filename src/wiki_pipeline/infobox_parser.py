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


_DATE_TEMPLATE_NAMES = {
    "birth date", "death date", "birth date and age", "death date and age",
    "birth-date", "death-date", "birth-date and age", "death-date and age",
    "b-da", "d-da",
}

_MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


def _resolve_date_template(raw: str) -> str | None:
    """Extract ISO date from date templates like {{birth date|Y|M|D}} or {{death date and age|3 March 1703|...}}."""
    parsed = mwparserfromhell.parse(raw)
    for t in parsed.filter_templates():
        name = str(t.name).strip().lower()
        if name not in _DATE_TEMPLATE_NAMES:
            continue
        positional = [str(p.value).strip() for p in t.params if str(p.name).strip().isdigit()]
        if not positional:
            continue

        # Try numeric Y|M|D: params are individual numbers
        nums = [p for p in positional if re.fullmatch(r"\d{1,4}", p)]
        if len(nums) >= 3:
            year, month, day = nums[0], int(nums[1]), int(nums[2])
            if 1 <= month <= 12 and 1 <= day <= 31:
                return f"{year}-{month:02d}-{day:02d}"

        # Try text date in first positional param: "3 March 1703"
        date = _parse_text_date(positional[0])
        if date:
            return date

    return None


def _parse_text_date(s: str) -> str | None:
    """Parse a text date like '3 March 1703' or 'March 3, 1703' to ISO."""
    s = s.strip()
    # DMY: "3 March 1703"
    m = re.match(r"(\d{1,2})\s+(\w+)\s+(\d{3,4})", s)
    if m:
        day, month_name, year = int(m.group(1)), m.group(2).lower(), m.group(3)
        mm = _MONTH_MAP.get(month_name)
        if mm:
            return f"{year}-{mm:02d}-{day:02d}"
    # MDY: "March 3, 1703"
    m = re.match(r"(\w+)\s+(\d{1,2}),?\s+(\d{3,4})", s)
    if m:
        month_name, day, year = m.group(1).lower(), int(m.group(2)), m.group(3)
        mm = _MONTH_MAP.get(month_name)
        if mm:
            return f"{year}-{mm:02d}-{day:02d}"
    return None
