"""Shared infobox extraction logic — generic field extraction from Wikipedia infoboxes."""

from __future__ import annotations

import re
from dataclasses import dataclass, field

import mwparserfromhell


@dataclass(frozen=True)
class InfoboxConfig:
    """Configuration for a category-specific infobox extractor."""
    infobox_names: frozenset[str]
    field_aliases: dict[str, list[str]]
    date_fields: frozenset[str] = frozenset()
    numeric_fields: frozenset[str] = frozenset()


def extract_infobox(
    wikitext: str,
    required_fields: tuple[str, ...] | list[str],
    config: InfoboxConfig,
) -> dict[str, str | None]:
    """Generic infobox field extraction using mwparserfromhell."""
    result: dict[str, str | None] = {f: None for f in required_fields}
    parsed = mwparserfromhell.parse(wikitext)

    infobox = None
    for t in parsed.filter_templates():
        name = t.name.strip().lower()
        if name in config.infobox_names:
            infobox = t
            break

    if infobox is None:
        return result

    for field_name in required_fields:
        aliases = config.field_aliases.get(field_name, [field_name])
        for alias in aliases:
            if infobox.has(alias):
                raw = str(infobox.get(alias).value).strip()
                cleaned = _clean_value(raw, field_name, config)
                if cleaned:
                    result[field_name] = cleaned
                    break

    return result


_DATE_TEMPLATE_NAMES = {
    "birth date", "death date", "birth date and age", "death date and age",
    "birth-date", "death-date", "birth-date and age", "death-date and age",
    "b-da", "d-da", "start date", "end date",
    "start date and age", "end date and age",
}

_MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


def _clean_value(raw: str, field_name: str, config: InfoboxConfig) -> str | None:
    """Clean a raw infobox value: resolve date templates, strip markup."""
    if not raw:
        return None

    if field_name in config.date_fields:
        date = _resolve_date_template(raw)
        if date:
            return date

    # Strip nested templates (up to 3 levels)
    cleaned = raw
    for _ in range(3):
        cleaned = re.sub(r"\{\{[^{}]*\}\}", "", cleaned)

    # Strip wikilinks: [[link|text]] -> text, [[link]] -> link
    cleaned = re.sub(r"\[\[(?:[^|\]]*\|)?([^\]]*)\]\]", r"\1", cleaned)
    # Strip [[File:...]] and [[Image:...]]
    cleaned = re.sub(r"\[\[(?:File|Image):[^\]]*\]\]", "", cleaned, flags=re.I)
    # Strip refs
    cleaned = re.sub(r"<ref[^>]*>.*?</ref>", "", cleaned, flags=re.DOTALL)
    cleaned = re.sub(r"<ref[^/]*/?>", "", cleaned)
    # Strip remaining HTML tags
    cleaned = re.sub(r"<[^>]+>", "", cleaned)
    # Strip HTML entities
    cleaned = cleaned.replace("&nbsp;", " ").replace("&ndash;", "–").replace("&mdash;", "—")
    cleaned = re.sub(r"&\w+;", "", cleaned)
    # Strip bold/italic markup
    cleaned = re.sub(r"'{2,5}", "", cleaned)
    # Strip stray template braces
    cleaned = cleaned.replace("}}", "").replace("{{", "")
    # Strip wikitext list markers and pixel sizes
    cleaned = re.sub(r"\d+px\b", "", cleaned)
    # Collapse newline lists into semicolon-separated values
    cleaned = re.sub(r"\s*\n\s*\*?\s*", "; ", cleaned)
    # Collapse whitespace
    cleaned = re.sub(r"\s+", " ", cleaned).strip().strip(",;").strip()

    if not cleaned:
        return None

    if field_name in config.numeric_fields:
        cleaned = cleaned.replace(",", "")

    return cleaned


def _resolve_date_template(raw: str) -> str | None:
    """Extract ISO date from date templates."""
    parsed = mwparserfromhell.parse(raw)
    for t in parsed.filter_templates():
        name = str(t.name).strip().lower()
        if name not in _DATE_TEMPLATE_NAMES:
            continue
        positional = [str(p.value).strip() for p in t.params if str(p.name).strip().isdigit()]
        if not positional:
            continue

        nums = [p for p in positional if re.fullmatch(r"\d{1,4}", p)]
        if len(nums) >= 3:
            year, month, day = nums[0], int(nums[1]), int(nums[2])
            if 1 <= month <= 12 and 1 <= day <= 31:
                return f"{year}-{month:02d}-{day:02d}"

        date = _parse_text_date(positional[0])
        if date:
            return date

    return None


def _parse_text_date(s: str) -> str | None:
    """Parse a text date like '3 March 1703' or 'March 3, 1703' to ISO."""
    s = s.strip()
    m = re.match(r"(\d{1,2})\s+(\w+)\s+(\d{3,4})", s)
    if m:
        day, month_name, year = int(m.group(1)), m.group(2).lower(), m.group(3)
        mm = _MONTH_MAP.get(month_name)
        if mm:
            return f"{year}-{mm:02d}-{day:02d}"
    m = re.match(r"(\w+)\s+(\d{1,2}),?\s+(\d{3,4})", s)
    if m:
        month_name, day, year = m.group(1).lower(), int(m.group(2)), m.group(3)
        mm = _MONTH_MAP.get(month_name)
        if mm:
            return f"{year}-{mm:02d}-{day:02d}"
    return None
