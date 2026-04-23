"""Biographical infobox field extraction."""

from __future__ import annotations

from .infobox_base import InfoboxConfig, extract_infobox

BIO_CONFIG = InfoboxConfig(
    infobox_names=frozenset({
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
        "infobox royalty",
        "infobox monarch",
        "infobox noble",
        "infobox pharaoh",
    }),
    field_aliases={
        "birth_date": ["birth_date", "born", "birthdate"],
        "death_date": ["death_date", "died", "deathdate"],
        "nationality": ["nationality", "citizenship", "country"],
        "occupation": ["occupation", "known_for", "notable_works"],
        "birth_place": ["birth_place", "birthplace", "born"],
        "death_place": ["death_place", "deathplace"],
    },
    date_fields=frozenset({"birth_date", "death_date"}),
)


def extract_infobox_fields(
    wikitext: str, required_fields: tuple[str, ...] | list[str]
) -> dict[str, str | None]:
    """Extract biographical fields from person-type infoboxes."""
    return extract_infobox(wikitext, required_fields, BIO_CONFIG)
