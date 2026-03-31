"""Tests for geo_infobox_parser — geographic infobox field extraction."""

from __future__ import annotations

from wiki_pipeline.geo_infobox_parser import extract_geo_infobox_fields

GEO_FIELDS = ("population", "area_km2", "elevation_m", "subdivision_name", "subdivision_type")


class TestExtractGeoInfoboxFields:
    def test_full_settlement_infobox(self):
        wikitext = """{{Infobox settlement
| name = Springfield
| population_total = 116250
| area_total_km2 = 159.97
| elevation_m = 180
| subdivision_name1 = Illinois
| subdivision_type1 = State
}}"""
        result = extract_geo_infobox_fields(wikitext, GEO_FIELDS)
        assert result["population"] == "116250"
        assert result["area_km2"] == "159.97"
        assert result["elevation_m"] == "180"
        assert result["subdivision_name"] == "Illinois"
        assert result["subdivision_type"] == "State"

    def test_city_infobox_variant(self):
        wikitext = """{{Infobox city
| name = Munich
| population_total = 1471508
| area_km2 = 310.7
| elevation_m = 520
| state = Bavaria
}}"""
        result = extract_geo_infobox_fields(wikitext, GEO_FIELDS)
        assert result["population"] == "1471508"
        assert result["area_km2"] == "310.7"
        assert result["subdivision_name"] == "Bavaria"

    def test_county_infobox(self):
        wikitext = """{{Infobox county
| pop = 5275541
| area_km2 = 2448
| state = [[Illinois]]
}}"""
        result = extract_geo_infobox_fields(wikitext, GEO_FIELDS)
        assert result["population"] == "5275541"
        assert result["area_km2"] == "2448"
        assert result["subdivision_name"] == "Illinois"

    def test_municipality_infobox(self):
        wikitext = """{{Infobox municipality
| population = 850000
| area = 785.6
| province = Barcelona
}}"""
        result = extract_geo_infobox_fields(wikitext, GEO_FIELDS)
        assert result["population"] == "850000"
        assert result["area_km2"] == "785.6"
        assert result["subdivision_name"] == "Barcelona"

    def test_partial_infobox(self):
        wikitext = """{{Infobox settlement
| population_total = 50000
}}"""
        result = extract_geo_infobox_fields(wikitext, GEO_FIELDS)
        assert result["population"] == "50000"
        assert result["area_km2"] is None
        assert result["elevation_m"] is None
        assert result["subdivision_name"] is None

    def test_no_infobox(self):
        wikitext = "'''Cook County''' is a county in Illinois."
        result = extract_geo_infobox_fields(wikitext, GEO_FIELDS)
        assert all(v is None for v in result.values())

    def test_non_geo_infobox_ignored(self):
        wikitext = """{{Infobox person
| name = John Doe
| birth_date = 1900
}}"""
        result = extract_geo_infobox_fields(wikitext, GEO_FIELDS)
        assert all(v is None for v in result.values())

    def test_wikilink_stripping(self):
        wikitext = """{{Infobox settlement
| subdivision_name1 = [[Illinois]]
| subdivision_type1 = [[U.S. state|State]]
}}"""
        result = extract_geo_infobox_fields(wikitext, GEO_FIELDS)
        assert result["subdivision_name"] == "Illinois"
        assert result["subdivision_type"] == "State"

    def test_ref_stripping(self):
        wikitext = """{{Infobox settlement
| population_total = 116250<ref>2020 census</ref>
}}"""
        result = extract_geo_infobox_fields(wikitext, ("population",))
        assert result["population"] == "116250"

    def test_empty_field_returns_none(self):
        wikitext = """{{Infobox settlement
| population_total =
}}"""
        result = extract_geo_infobox_fields(wikitext, ("population",))
        assert result["population"] is None

    def test_comma_in_population_stripped(self):
        wikitext = """{{Infobox settlement
| population_total = 1,471,508
}}"""
        result = extract_geo_infobox_fields(wikitext, ("population",))
        assert result["population"] == "1471508"

    def test_subset_of_fields(self):
        wikitext = """{{Infobox settlement
| population_total = 50000
| elevation_m = 200
}}"""
        result = extract_geo_infobox_fields(wikitext, ("population", "elevation_m"))
        assert result["population"] == "50000"
        assert result["elevation_m"] == "200"
        assert "area_km2" not in result

    def test_district_infobox(self):
        wikitext = """{{Infobox district
| population_total = 300000
| area_km2 = 120.5
| region = Central Region
}}"""
        result = extract_geo_infobox_fields(wikitext, GEO_FIELDS)
        assert result["population"] == "300000"
        assert result["subdivision_name"] == "Central Region"

    def test_place_infobox(self):
        wikitext = """{{Infobox place
| population = 12000
| elevation_m = 450
| state = Vermont
}}"""
        result = extract_geo_infobox_fields(wikitext, GEO_FIELDS)
        assert result["population"] == "12000"
        assert result["elevation_m"] == "450"
        assert result["subdivision_name"] == "Vermont"
