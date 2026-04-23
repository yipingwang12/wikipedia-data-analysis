"""NLP-based text extractor — regex extraction from Wikipedia first-sentence patterns."""

from __future__ import annotations

import re

MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}

NATIONALITIES = frozenset({
    "afghan", "albanian", "algerian", "american", "andorran", "angolan",
    "argentine", "argentinian", "armenian", "australian", "austrian",
    "azerbaijani", "bahamian", "bahraini", "bangladeshi", "barbadian",
    "belarusian", "belgian", "belizean", "beninese", "bhutanese",
    "bolivian", "bosnian", "botswanan", "brazilian", "british",
    "bruneian", "bulgarian", "burkinabe", "burmese", "burundian",
    "cambodian", "cameroonian", "canadian", "cape verdean", "central african",
    "chadian", "chilean", "chinese", "colombian", "comorian",
    "congolese", "costa rican", "croatian", "cuban", "cypriot",
    "czech", "danish", "djiboutian", "dominican", "dutch",
    "east timorese", "ecuadorian", "egyptian", "emirati", "english",
    "equatorial guinean", "eritrean", "estonian", "ethiopian", "fijian",
    "filipino", "finnish", "flemish", "french", "gabonese",
    "gambian", "georgian", "german", "ghanaian", "greek",
    "grenadian", "guatemalan", "guinean", "guyanese", "haitian",
    "honduran", "hungarian", "icelandic", "indian", "indonesian",
    "iranian", "iraqi", "irish", "israeli", "italian",
    "ivorian", "jamaican", "japanese", "jordanian", "kazakh",
    "kenyan", "korean", "kosovar", "kuwaiti", "kyrgyz",
    "laotian", "latvian", "lebanese", "liberian", "libyan",
    "liechtensteiner", "lithuanian", "luxembourgish", "macedonian", "malagasy",
    "malawian", "malaysian", "maldivian", "malian", "maltese",
    "mauritanian", "mauritian", "mexican", "moldovan", "monacan",
    "mongolian", "montenegrin", "moroccan", "mozambican", "namibian",
    "nepalese", "new zealand", "nicaraguan", "nigerian", "nigerien",
    "north korean", "norwegian", "omani", "ottoman", "pakistani",
    "palauan", "palestinian", "panamanian", "papua new guinean", "paraguayan",
    "persian", "peruvian", "polish", "portuguese", "prussian",
    "puerto rican", "qatari", "romanian", "roman", "russian",
    "rwandan", "salvadoran", "samoan", "saudi", "scottish",
    "senegalese", "serbian", "sierra leonean", "singaporean", "slovak",
    "slovenian", "somali", "south african", "south korean", "soviet",
    "spanish", "sri lankan", "sudanese", "surinamese", "swazi",
    "swedish", "swiss", "syrian", "taiwanese", "tajik",
    "tanzanian", "thai", "togolese", "tongan", "trinidadian",
    "tunisian", "turkish", "turkmen", "ugandan", "ukrainian",
    "uruguayan", "uzbek", "venezuelan", "vietnamese", "welsh",
    "yemeni", "zambian", "zimbabwean",
    # Historical
    "byzantine", "austro-hungarian", "czechoslovak", "yugoslav",
})

OCCUPATIONS = frozenset({
    "activist", "actor", "actress", "admiral", "alpinist", "ambassador",
    "anthropologist", "archaeologist", "architect", "artist", "astronaut",
    "astronomer", "athlete", "author", "aviator",
    "banker", "biologist", "bishop", "botanist", "boxer", "broadcaster",
    "businessman", "businesswoman",
    "cardinal", "cartographer", "cellist", "chancellor", "chef",
    "chemist", "choreographer", "cinematographer", "clergyman", "coach",
    "collector", "comedian", "commander", "commentator", "composer",
    "conductor", "congressman", "consultant", "courtier", "critic",
    "cryptographer", "curator", "cyclist",
    "dancer", "designer", "diplomat", "director", "dramatist",
    "economist", "editor", "educator", "emperor", "empress",
    "engineer", "engraver", "entertainer", "entrepreneur", "essayist",
    "ethnographer", "evangelist", "explorer",
    "feminist", "fencer", "filmmaker", "financier",
    "general", "geneticist", "geographer", "geologist", "golfer",
    "governor", "guitarist",
    "historian",
    "illustrator", "imam", "inventor",
    "journalist", "judge", "jurist",
    "king",
    "lawyer", "lexicographer", "librettist", "linguist", "lyricist",
    "magistrate", "mathematician", "mayor", "medic", "memoirist",
    "merchant", "meteorologist", "microbiologist", "military officer",
    "minister", "missionary", "monarch", "monk", "mountaineer",
    "musician", "musicologist", "mystic",
    "naturalist", "navigator", "neuroscientist", "nobleman", "novelist",
    "nun", "nurse",
    "officer", "organist",
    "painter", "paleontologist", "pastor", "patron", "pedagogue",
    "philanthropist", "philosopher", "photographer", "physician",
    "physicist", "pianist", "pilot", "playwright", "poet",
    "political scientist", "politician", "polymath", "pope", "preacher",
    "president", "priest", "prince", "princess", "printmaker",
    "producer", "professor", "programmer", "psychiatrist", "psychologist",
    "publisher",
    "queen",
    "rabbi", "racer", "reformer", "regent", "researcher",
    "revolutionary", "ruler",
    "sailor", "saint", "scholar", "scientist", "screenwriter",
    "sculptor", "senator", "singer", "singer-songwriter", "sociologist",
    "soldier", "songwriter", "spy", "statesman", "stateswoman",
    "sultan", "surgeon", "swimmer",
    "teacher", "theologian", "trader", "translator",
    "violinist", "vocalist",
    "warrior", "writer",
    "zoologist",
})

# ---------------------------------------------------------------------------
# Date patterns
# ---------------------------------------------------------------------------

_MONTH_RE = r"(?:January|February|March|April|May|June|July|August|September|October|November|December)"

# Full date: "14 November 1840" or "November 14, 1840"
_FULL_DATE = (
    rf"(?:(?:\d{{1,2}}\s+{_MONTH_RE}\s+\d{{3,4}})"  # DMY: 14 November 1840
    rf"|(?:{_MONTH_RE}\s+\d{{1,2}},?\s+\d{{3,4}}))"  # MDY: November 14, 1840
)

# Date that may be circa or year-only
_DATE = rf"(?:c\.\s*)?(?:{_FULL_DATE}|\d{{3,4}})"

_SEP = r"\s*[\u2013\u2014\-]\s*"  # en-dash, em-dash, or hyphen

# (DATE – DATE) with optional "; aged N" suffix
_BIRTH_DEATH_RE = re.compile(
    rf"\(({_DATE}){_SEP}({_DATE})(?:\s*;[^)]*?)?\)",
    re.IGNORECASE,
)

# (born DATE – DATE) — approximate birth with known death
_BORN_DEATH_RE = re.compile(
    rf"\(\s*born\s+({_DATE}){_SEP}({_DATE})(?:\s*;[^)]*?)?\)",
    re.IGNORECASE,
)

# (born DATE)
_BORN_RE = re.compile(
    rf"\(\s*born\s+({_DATE})\s*\)",
    re.IGNORECASE,
)

# For fallback: find any date-like string within text
_ANY_DATE_RE = re.compile(rf"(?:c\.\s*)?(?:{_FULL_DATE}|\d{{3,4}})", re.IGNORECASE)

# Separator with surrounding whitespace (for splitting parenthetical halves)
_MAIN_SEP_RE = re.compile(r"\s+[\u2013\u2014\-]\s+")

# "was a/an/the ..." or "is a/an/the ..." clause
_WAS_A_RE = re.compile(
    r"(?:was|is)\s+(?:a|an|the)\s+(.*?)(?:\s+who\b|\s+that\b|\s+whose\b|\s+best\b|\s+widely\b|\s+known\b|\.\s|,\s+(?:who\b|whose\b|born\b|and\s+(?:the\b|a\b|founder\b|one\b|co-))|$)",
    re.IGNORECASE,
)


def normalize_date_with_note(raw: str | None) -> tuple[str | None, str | None]:
    """Normalize a date, returning (normalized_date, note).

    Note is non-None when the date is an approximation (decade/century).
    """
    if not raw:
        return None, None
    s = raw.strip()

    # Strip circa prefix for matching, but preserve in note
    s_clean = re.sub(r"^[Cc]\.?\s*", "", s).strip()

    # Decade approximations: "1900s", "early 1960s", "late 1950s", "c. 1900s"
    m = re.match(r"^(early|late|mid)?\s*(\d{3,4})s$", s_clean, re.I)
    if m:
        qualifier, decade = m.group(1), int(m.group(2))
        return f"{decade}-01-01", f"~{s}"

    # Century references: "7 century", "19-century", "c. 7th century"
    m = re.match(r"^(\d{1,2})(?:st|nd|rd|th)?[- ]?century$", s_clean, re.I)
    if m:
        century = int(m.group(1))
        year = (century - 1) * 100 + 1
        return f"{year:04d}-01-01", f"~{s}"

    # Month + day only (no year): "January 10", "19 November", "November, 12"
    m = re.match(rf"^({_MONTH_RE}),?\s+(\d{{1,2}})$", s, re.IGNORECASE)
    if m:
        month_name, day = m.group(1).lower(), int(m.group(2))
        return f"9999-{MONTH_MAP[month_name]:02d}-{day:02d}", f"~{raw.strip()} (no year)"
    m = re.match(rf"^(\d{{1,2}})\s+({_MONTH_RE})$", s, re.IGNORECASE)
    if m:
        day, month_name = int(m.group(1)), m.group(2).lower()
        return f"9999-{MONTH_MAP[month_name]:02d}-{day:02d}", f"~{raw.strip()} (no year)"

    date = normalize_date(s)
    return date, None


def normalize_date(raw: str | None) -> str | None:
    """Normalize a date string to ISO YYYY-MM-DD, YYYY-MM, year-only, or c. year.

    Handles: text dates (MDY/DMY), aged/age suffixes, HTML entities, ordinals,
    date ranges, circa prefixes, and extra commas/whitespace.
    """
    if not raw:
        return None
    s = raw.strip()

    # Clean HTML entities and markup
    s = s.replace("&ndash;", "-").replace("&nbsp;", " ")
    s = re.sub(r"&\w+;", "", s)

    # Strip template junk: "965}} ()}}..." → "965"
    if "}}" in s:
        s = s.split("}}")[0].strip()

    # Strip aged/age suffixes: "(aged 72)", "(age 72)"
    s = re.sub(r"\s*[,(]?\s*aged\s+(?:about\s+)?\d+[^)]*\)?\s*,?\s*$", "", s, flags=re.I)
    s = re.sub(r"\s*\(age\s+\d+[^)]*\)\s*$", "", s, flags=re.I)

    # Strip ordinal suffixes: 1st → 1, 2nd → 2
    s = re.sub(r"(\d+)(?:st|nd|rd|th)\b", r"\1", s, flags=re.I)

    # Strip leading prefixes: "On", "Possibly", "Died", "Birth date", "Either"
    s = re.sub(r"^(?:On|Possibly|Died|Birth date|Either)\s+", "", s, flags=re.I)

    # Strip leading "c." or "c "
    circa = ""
    if re.match(r"^[Cc]\.\s*", s):
        circa = "c. "
        s = re.sub(r"^[Cc]\.\s*", "", s)

    # Strip "or ..." alternatives: "1608 or 1609" → "1608"
    s = re.sub(r"\s+or\s+.*$", "", s)

    # Strip comma-separated year alternatives: "1946, 1947" → "1946"
    # Only when the part before the comma is a bare year
    m_alt = re.match(r"^(\d{3,4}),\s+", s)
    if m_alt:
        s = m_alt.group(1)

    # Collapse whitespace, strip trailing commas
    s = re.sub(r"\s+", " ", s).strip().rstrip(",").strip()

    # Already ISO?
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return f"{circa}{s}" if circa else s

    # Incomplete ISO: "1908-6-24" → zero-pad
    m = re.fullmatch(r"(\d{4})-(\d{1,2})-(\d{1,2})", s)
    if m:
        return f"{circa}{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"

    # Year-only: "1840"
    if re.fullmatch(r"\d{3,4}", s):
        return f"{circa}{s}"

    # Date range "1840/42" or "1840-42" → take first year
    m = re.match(r"^(\d{3,4})[-/]\d{2,4}$", s)
    if m:
        return f"{circa}{m.group(1)}"

    # Numeric DD-MM-YYYY, DD/MM/YYYY, or DD.MM.YYYY
    m = re.fullmatch(r"(\d{1,2})[-/.](\d{1,2})[-/.](\d{4})", s)
    if m:
        day, month, year = int(m.group(1)), int(m.group(2)), m.group(3)
        if 1 <= month <= 12 and 1 <= day <= 31:
            return f"{circa}{year}-{month:02d}-{day:02d}"

    # BC/BCE suffix — extract before text date matching
    bc = ""
    m_bc = re.search(r"\s*B\.?C\.?E?\.?\s*$", s, re.I)
    if m_bc:
        bc = " BC"
        s = s[:m_bc.start()].strip()
        # Year-only BC: "620", "43"
        if re.fullmatch(r"\d{1,4}", s):
            return f"{circa}{s}{bc}"

    # DMY: "14 November 1840", "14 November, 1840", "14 of November, 1840"
    m = re.match(rf"(\d{{1,2}})\s+(?:of\s+)?({_MONTH_RE}),?\s*(\d{{1,4}})", s, re.IGNORECASE)
    if m:
        day, month_name, year = int(m.group(1)), m.group(2).lower(), m.group(3)
        return f"{circa}{year}-{MONTH_MAP[month_name]:02d}-{day:02d}{bc}"

    # MDY: "November 14, 1840", "November 14,1840" (require comma or space+4-digit year)
    m = re.match(rf"({_MONTH_RE})\s+(\d{{1,2}}),\s*(\d{{1,4}})", s, re.IGNORECASE)
    if not m:
        m = re.match(rf"({_MONTH_RE})\s+(\d{{1,2}})\s+(\d{{4}})", s, re.IGNORECASE)
    if m:
        month_name, day, year = m.group(1).lower(), int(m.group(2)), m.group(3)
        return f"{circa}{year}-{MONTH_MAP[month_name]:02d}-{day:02d}{bc}"

    # Month + year: "November 1840", "February 341 BC"
    m = re.match(rf"({_MONTH_RE})\s+(\d{{1,4}})$", s, re.IGNORECASE)
    if m:
        month_name, year = m.group(1).lower(), m.group(2)
        return f"{circa}{year}-{MONTH_MAP[month_name]:02d}{bc}"

    # If we stripped BC but couldn't parse the rest, restore it
    if bc:
        return f"{circa}{s}{bc}" if s else None

    # Drop values that are clearly not dates (no digits, or too short/nonsensical)
    if not s or not re.search(r"\d", s):
        return None

    return s


# Keep private alias for internal use
_normalize_date = normalize_date


def _extract_first_paragraph(plain_text: str) -> str:
    """Return first paragraph or first 500 chars, whichever is shorter."""
    chunk = plain_text[:500]
    idx = chunk.find("\n\n")
    if idx != -1:
        return chunk[:idx]
    return chunk


def _date_sort_key(normalized: str) -> tuple[int, int, int]:
    """Extract (year, month, day) from normalized date for comparison."""
    clean = normalized.removeprefix("c. ")
    parts = clean.split("-")
    year = int(parts[0])
    month = int(parts[1]) if len(parts) > 1 else 0
    day = int(parts[2]) if len(parts) > 2 else 0
    return (year, month, day)


def _pick_earliest(dates: list[str]) -> str:
    return min(dates, key=_date_sort_key)


def _pick_latest(dates: list[str]) -> str:
    return max(dates, key=_date_sort_key)


def _find_and_normalize_dates(text: str) -> list[str]:
    """Find all date-like strings in text, return normalized list."""
    return [d for d in (_normalize_date(m.group()) for m in _ANY_DATE_RE.finditer(text)) if d]


def _extract_dates_fallback(text: str) -> tuple[str | None, str | None]:
    """Fallback: extract dates from first parenthetical, handling places and 'or'."""
    paren = re.search(r"\(([^)]+)\)", text)
    if not paren:
        return None, None

    content = paren.group(1)
    parts = _MAIN_SEP_RE.split(content, maxsplit=1)

    if len(parts) == 2:
        birth_dates = _find_and_normalize_dates(parts[0])
        death_dates = _find_and_normalize_dates(parts[1])
        birth = _pick_earliest(birth_dates) if birth_dates else None
        death = _pick_latest(death_dates) if death_dates else None
        return birth, death

    # No separator — all dates treated as birth candidates
    dates = _find_and_normalize_dates(content)
    return (_pick_earliest(dates) if dates else None), None


def _extract_dates(text: str) -> tuple[str | None, str | None]:
    """Extract (birth_date, death_date) from parenthetical in text."""
    m = _BIRTH_DEATH_RE.search(text)
    if m:
        return _normalize_date(m.group(1)), _normalize_date(m.group(2))

    m = _BORN_DEATH_RE.search(text)
    if m:
        return _normalize_date(m.group(1)), _normalize_date(m.group(2))

    m = _BORN_RE.search(text)
    if m:
        return _normalize_date(m.group(1)), None

    return _extract_dates_fallback(text)


def _extract_nationality(clause: str) -> str | None:
    """Extract nationality from a 'was a/an ...' clause."""
    if not clause:
        return None

    found: list[str] = []
    # Tokenize: split on spaces and "and"
    # Handle "Polish and naturalised-French" -> ["Polish", "naturalised-French"]
    tokens = re.split(r"\s+", clause.strip())

    for token in tokens:
        lower = token.lower().rstrip(".,;")
        if lower == "and":
            continue

        # Handle hyphenated forms: "German-born" -> "German", "naturalised-French" -> "French"
        parts = lower.split("-")
        for part in parts:
            if part in NATIONALITIES:
                found.append(part.capitalize())
                break

        # Check two-word nationalities with next token
        # (handled by checking the full lowered token against multi-word entries)
        if lower in NATIONALITIES:
            cap = " ".join(w.capitalize() for w in lower.split())
            if cap not in found:
                found.append(cap)

        # Stop once we hit a known occupation (we've left the nationality zone)
        if lower.rstrip(".,;") in OCCUPATIONS or lower.rstrip("s") in OCCUPATIONS:
            break

    # Deduplicate while preserving order
    seen: set[str] = set()
    deduped: list[str] = []
    for n in found:
        if n not in seen:
            seen.add(n)
            deduped.append(n)

    return ", ".join(deduped) if deduped else None


def _extract_occupation(clause: str) -> str | None:
    """Extract occupation from a 'was a/an ...' clause."""
    if not clause:
        return None

    found: list[str] = []
    tokens = re.split(r"\s+", clause.strip())

    i = 0
    while i < len(tokens):
        token = tokens[i]
        lower = token.lower().rstrip(".,;")

        # Skip nationalities and connectors
        if lower == "and" or lower == "or":
            # "and" between occupations: look ahead
            if found:
                i += 1
                continue
            # "and" between nationalities: skip
            i += 1
            continue

        # Check hyphenated as nationality
        parts = lower.split("-")
        is_nat = any(p in NATIONALITIES for p in parts)
        # Also skip suffixes like "born", "naturalised", "naturalized"
        is_suffix = any(p in ("born", "naturalised", "naturalized") for p in parts)

        if is_nat or is_suffix:
            # Pure nationality token, skip
            if all(p in NATIONALITIES or p in ("born", "naturalised", "naturalized", "and") for p in parts):
                i += 1
                continue

        # Check for hyphenated occupation (e.g., "singer-songwriter")
        if lower in OCCUPATIONS:
            found.append(lower)
            i += 1
            continue

        # Check if this token is a known occupation
        if lower in OCCUPATIONS:
            found.append(lower)
            i += 1
            continue

        # Check for qualifier + occupation: "theoretical physicist"
        if i + 1 < len(tokens):
            next_lower = tokens[i + 1].lower().rstrip(".,;")
            if next_lower in OCCUPATIONS and lower not in NATIONALITIES:
                found.append(f"{lower} {next_lower}")
                i += 2
                continue

        i += 1

    return ", ".join(found) if found else None


def extract_from_text(
    plain_text: str,
    existing: dict[str, str | None],
    required_fields: tuple[str, ...] | list[str],
) -> dict[str, str | None]:
    """Fill None-valued fields using regex extraction from Wikipedia first-sentence patterns.

    Returns merged dict with existing values preserved and gaps filled where possible.
    """
    missing = [f for f in required_fields if existing.get(f) is None]
    if not missing:
        return dict(existing)

    first_para = _extract_first_paragraph(plain_text)
    result = dict(existing)

    # Extract dates if needed
    need_birth = "birth_date" in missing
    need_death = "death_date" in missing
    if need_birth or need_death:
        birth, death = _extract_dates(first_para)
        if need_birth and birth is not None:
            result["birth_date"] = birth
        if need_death and death is not None:
            result["death_date"] = death

    # Extract nationality and occupation from "was a/an ..." clause
    need_nat = "nationality" in missing
    need_occ = "occupation" in missing
    if need_nat or need_occ:
        m = _WAS_A_RE.search(first_para)
        if m:
            clause = m.group(1)
            if need_nat:
                result["nationality"] = _extract_nationality(clause)
            if need_occ:
                result["occupation"] = _extract_occupation(clause)

        # Fallback: "of [nationality] origin/descent/birth/heritage"
        if need_nat and result.get("nationality") is None:
            m2 = re.search(
                r"\bof\s+(\w+)\s+(?:origin|descent|birth|heritage|extraction|ancestry)\b",
                first_para, re.IGNORECASE,
            )
            if m2 and m2.group(1).lower() in NATIONALITIES:
                result["nationality"] = m2.group(1).capitalize()

    return result
