"""Category-specific infobox extractors for non-biographical/non-geographic categories."""

from __future__ import annotations

from .infobox_base import InfoboxConfig, extract_infobox

# --- Battles, wars, conflicts ---

BATTLE_CONFIG = InfoboxConfig(
    infobox_names=frozenset({
        "infobox military conflict",
        "infobox civilian attack",
        "infobox war faction",
    }),
    field_aliases={
        "date": ["date"],
        "location": ["place", "location"],
        "belligerents": ["combatant1", "combatant2", "combatants"],
        "result": ["result", "outcome"],
        "casualties": ["casualties1", "casualties2", "casualties"],
        "commanders": ["commander1", "commander2"],
    },
    date_fields=frozenset({"date"}),
)

BATTLE_FIELDS = ("date", "location", "belligerents", "result", "casualties", "commanders")


def extract_battle_fields(
    wikitext: str, required_fields: tuple[str, ...] | list[str] = BATTLE_FIELDS
) -> dict[str, str | None]:
    return extract_infobox(wikitext, required_fields, BATTLE_CONFIG)


# --- Explorations, voyages, spacecraft ---

EXPLORATION_CONFIG = InfoboxConfig(
    infobox_names=frozenset({
        "infobox spaceflight",
        "infobox spacecraft class",
        "infobox space station",
        "infobox spaceflight/dock",
        "infobox expedition",
        "infobox mission",
    }),
    field_aliases={
        "date": ["launch_date", "date", "start_date"],
        "destination": ["destination", "target"],
        "origin": ["launch_site", "origin"],
        "crew": ["crew_members", "crew_size", "crew", "operator"],
        "mission_type": ["mission_type", "type"],
        "status": ["status", "disposition", "outcome"],
    },
    date_fields=frozenset({"date"}),
)

EXPLORATION_FIELDS = ("date", "destination", "origin", "crew", "mission_type", "status")


def extract_exploration_fields(
    wikitext: str, required_fields: tuple[str, ...] | list[str] = EXPLORATION_FIELDS
) -> dict[str, str | None]:
    return extract_infobox(wikitext, required_fields, EXPLORATION_CONFIG)


# --- Astronomy ---

ASTRONOMY_CONFIG = InfoboxConfig(
    infobox_names=frozenset({
        "infobox planet",
        "infobox star",
        "infobox galaxy",
        "infobox nebula",
        "infobox constellation",
        "infobox comet",
        "infobox asteroid",
        "infobox minor planet",
        "infobox pulsar",
    }),
    field_aliases={
        "type": ["type", "spectral", "mp_category"],
        "distance": ["dist_ly", "dist_pc", "distance", "aphelion"],
        "mass": ["mass", "launch_mass"],
        "radius": ["radius", "mean_radius"],
        "constellation": ["constellation", "const"],
        "discovery_date": ["discovered", "discovery_date", "discovery"],
        "orbital_period": ["period", "orbit_period", "orbital_period", "p_orbit"],
        "rotational_period": ["rotation", "rotational_period", "sidereal_day"],
    },
    date_fields=frozenset({"discovery_date"}),
)

ASTRONOMY_FIELDS = (
    "type", "distance", "mass", "radius", "constellation",
    "discovery_date", "orbital_period", "rotational_period",
)


def extract_astronomy_fields(
    wikitext: str, required_fields: tuple[str, ...] | list[str] = ASTRONOMY_FIELDS
) -> dict[str, str | None]:
    return extract_infobox(wikitext, required_fields, ASTRONOMY_CONFIG)


# --- Biology ---

BIOLOGY_CONFIG = InfoboxConfig(
    infobox_names=frozenset({
        "speciesbox",
        "taxobox",
        "infobox disease",
        "infobox organism",
    }),
    field_aliases={
        "type": ["regnum", "phylum", "classis", "ordo"],
        "scientific_name": ["binomial", "taxon", "binomial_authority"],
        "conservation_status": ["status"],
        "habitat": ["habitat"],
        "distribution": ["range", "range_description", "range_map_caption"],
    },
)

BIOLOGY_FIELDS = ("type", "scientific_name", "conservation_status", "habitat", "distribution")


def extract_biology_fields(
    wikitext: str, required_fields: tuple[str, ...] | list[str] = BIOLOGY_FIELDS
) -> dict[str, str | None]:
    return extract_infobox(wikitext, required_fields, BIOLOGY_CONFIG)


# --- Mathematics / statistics ---

MATH_CONFIG = InfoboxConfig(
    infobox_names=frozenset({
        "infobox mathematical statement",
        "infobox mathematics",
        "infobox theorem",
        "infobox conjecture",
        "infobox equation",
    }),
    field_aliases={
        "field": ["field", "branch", "area"],
        "year_discovered": ["year", "conjectured", "proved", "date"],
        "discoverer": ["conjectured by", "proved by", "discoverer", "author"],
        "related_to": ["generalizations", "consequences", "related"],
    },
    date_fields=frozenset({"year_discovered"}),
)

MATH_FIELDS = ("field", "year_discovered", "discoverer", "related_to")


def extract_math_fields(
    wikitext: str, required_fields: tuple[str, ...] | list[str] = MATH_FIELDS
) -> dict[str, str | None]:
    return extract_infobox(wikitext, required_fields, MATH_CONFIG)


# --- Registry: pattern file stem → (extract_fn, default_fields) ---

EXTRACTOR_REGISTRY: dict[str, tuple] = {
    "battles_wars_conflicts": (extract_battle_fields, BATTLE_FIELDS),
    "explorations_voyages_spacecraft": (extract_exploration_fields, EXPLORATION_FIELDS),
    "astronomy": (extract_astronomy_fields, ASTRONOMY_FIELDS),
    "biology": (extract_biology_fields, BIOLOGY_FIELDS),
    "mathematics_statistics": (extract_math_fields, MATH_FIELDS),
}
