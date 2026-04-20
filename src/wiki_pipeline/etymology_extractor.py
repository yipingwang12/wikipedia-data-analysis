"""Extract etymology/name-origin text from Wikipedia article sections."""

from __future__ import annotations

import re

import mwparserfromhell

_ETYMOLOGY_HEADING_RE = re.compile(
    r'(?i)^(etymology|name(?:\s+origin)?|origin(?:\s+of\s+(?:the\s+)?name)?|'
    r'naming|history\s+and\s+name|name\s+and\s+history|'
    r'name\s+and\s+etymology|derivation(?:\s+of\s+(?:the\s+)?name)?)$'
)

_LEAD_ETYMOLOGY_RE = re.compile(
    r'(?:named?\s+(?:after|for|from)|derives?\s+from|'
    r'the\s+name\s+(?:comes?\s+from|derives?\s+from|means?|is\s+derived|originates?)|'
    r'its\s+name\s+(?:comes?\s+from|means?)|named\s+in\s+honor)',
    re.IGNORECASE,
)


def extract_etymology_section(wikitext: str) -> str | None:
    """Return text of the first etymology/name section, or None."""
    if not wikitext:
        return None
    wikicode = mwparserfromhell.parse(wikitext)
    for section in wikicode.get_sections(levels=[2, 3], include_headings=True):
        headings = section.filter_headings()
        if not headings:
            continue
        heading_text = headings[0].title.strip_code().strip()
        if _ETYMOLOGY_HEADING_RE.match(heading_text):
            content = section.strip_code().strip()
            lines = [l for l in content.splitlines()
                     if l.strip() and not re.match(r'^=+', l.strip())]
            result = ' '.join(lines).strip()
            if result:
                return result[:2000]
    return None


def extract_etymology_from_lead(plaintext: str) -> str | None:
    """Extract the etymology sentence(s) from the article lead paragraph."""
    if not plaintext:
        return None
    lead = plaintext[:1500]
    match = _LEAD_ETYMOLOGY_RE.search(lead)
    if not match:
        return None
    start = lead.rfind('.', 0, match.start())
    end = lead.find('.', match.end())
    sentence = lead[start + 1: end + 1 if end != -1 else len(lead)].strip()
    return sentence if len(sentence) > 20 else None


def extract_etymology_fields(
    wikitext: str,
    required_fields: tuple[str, ...] | list[str] = ("etymology",),
) -> dict[str, str | None]:
    """Primary etymology extractor: section parser over wikitext."""
    return {"etymology": extract_etymology_section(wikitext)}
