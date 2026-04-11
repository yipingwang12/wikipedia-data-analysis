"""Tests for category-specific infobox extractors."""

from __future__ import annotations

from wiki_pipeline.extractors import (
    extract_battle_fields,
    extract_exploration_fields,
    extract_astronomy_fields,
    extract_biology_fields,
    extract_math_fields,
    BATTLE_FIELDS,
    EXPLORATION_FIELDS,
    ASTRONOMY_FIELDS,
    BIOLOGY_FIELDS,
    MATH_FIELDS,
)


class TestBattleExtractor:
    def test_military_conflict(self):
        wikitext = (
            "{{Infobox military conflict\n"
            "| conflict = Battle of Gettysburg\n"
            "| date = July 1–3, 1863\n"
            "| place = Gettysburg, Pennsylvania\n"
            "| result = Union victory\n"
            "| combatant1 = United States\n"
            "| combatant2 = Confederate States\n"
            "| commander1 = George Meade\n"
            "| commander2 = Robert E. Lee\n"
            "| casualties1 = 23,049\n"
            "| casualties2 = 23,231\n"
            "}}"
        )
        result = extract_battle_fields(wikitext)
        assert result["location"] == "Gettysburg, Pennsylvania"
        assert result["result"] == "Union victory"
        assert result["commanders"] == "George Meade"

    def test_no_infobox(self):
        result = extract_battle_fields("No infobox here.")
        assert all(v is None for v in result.values())

    def test_subset_fields(self):
        wikitext = "{{Infobox military conflict|place=Berlin}}"
        result = extract_battle_fields(wikitext, ("location",))
        assert result["location"] == "Berlin"
        assert "result" not in result


class TestExplorationExtractor:
    def test_spaceflight(self):
        wikitext = (
            "{{Infobox spaceflight\n"
            "| name = Apollo 11\n"
            "| mission_type = Crewed lunar landing\n"
            "| launch_date = July 16, 1969\n"
            "| launch_site = Kennedy Space Center\n"
            "| operator = NASA\n"
            "| status = Complete\n"
            "}}"
        )
        result = extract_exploration_fields(wikitext)
        assert result["mission_type"] == "Crewed lunar landing"
        assert result["origin"] == "Kennedy Space Center"
        assert result["crew"] == "NASA"
        assert result["status"] == "Complete"

    def test_no_infobox(self):
        result = extract_exploration_fields("No infobox here.")
        assert all(v is None for v in result.values())


class TestAstronomyExtractor:
    def test_planet(self):
        wikitext = (
            "{{Infobox planet\n"
            "| name = Ceres\n"
            "| discovered = January 1, 1801\n"
            "| discoverer = Giuseppe Piazzi\n"
            "| mp_category = dwarf planet\n"
            "| period = 4.60 yr\n"
            "| rotation = 9.074170 h\n"
            "| mean_radius = 473 km\n"
            "}}"
        )
        result = extract_astronomy_fields(wikitext)
        assert result["type"] == "dwarf planet"
        assert result["orbital_period"] == "4.60 yr"
        assert result["rotational_period"] == "9.074170 h"
        assert result["radius"] == "473 km"

    def test_no_infobox(self):
        result = extract_astronomy_fields("No infobox here.")
        assert all(v is None for v in result.values())


class TestBiologyExtractor:
    def test_speciesbox(self):
        wikitext = (
            "{{Speciesbox\n"
            "| taxon = Panthera leo\n"
            "| authority = Linnaeus, 1758\n"
            "| status = VU\n"
            "| status_system = IUCN3.1\n"
            "| genus = Panthera\n"
            "| species = leo\n"
            "}}"
        )
        result = extract_biology_fields(wikitext)
        assert result["scientific_name"] == "Panthera leo"
        assert result["conservation_status"] == "VU"

    def test_taxobox(self):
        wikitext = (
            "{{Taxobox\n"
            "| name = Gray wolf\n"
            "| regnum = Animalia\n"
            "| binomial = Canis lupus\n"
            "| status = LC\n"
            "}}"
        )
        result = extract_biology_fields(wikitext)
        assert result["type"] == "Animalia"
        assert result["scientific_name"] == "Canis lupus"
        assert result["conservation_status"] == "LC"

    def test_no_infobox(self):
        result = extract_biology_fields("No infobox here.")
        assert all(v is None for v in result.values())


class TestMathExtractor:
    def test_no_infobox(self):
        result = extract_math_fields("The Pythagorean theorem states that...")
        assert all(v is None for v in result.values())

    def test_returns_all_fields(self):
        result = extract_math_fields("No infobox.")
        assert set(result.keys()) == set(MATH_FIELDS)
