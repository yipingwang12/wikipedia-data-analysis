"""Geographic infobox field extraction."""

from __future__ import annotations

from .infobox_base import InfoboxConfig, extract_infobox

GEO_CONFIG = InfoboxConfig(
    infobox_names=frozenset({
        "infobox settlement",
        "infobox city",
        "infobox municipality",
        "infobox county",
        "infobox district",
        "infobox place",
    }),
    field_aliases={
        "population": ["population_total", "pop", "population"],
        "area_km2": ["area_km2", "area_total_km2", "area"],
        "elevation_m": ["elevation_m", "elevation"],
        "subdivision_name": ["subdivision_name1", "state", "region", "province"],
        "subdivision_type": ["subdivision_type1"],
    },
    numeric_fields=frozenset({"population", "area_km2", "elevation_m"}),
)


def extract_geo_infobox_fields(
    wikitext: str, required_fields: tuple[str, ...] | list[str]
) -> dict[str, str | None]:
    """Extract geographic fields from settlement/city/county infoboxes."""
    return extract_infobox(wikitext, required_fields, GEO_CONFIG)
