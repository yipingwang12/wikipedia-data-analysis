"""Tests for infobox_parser — mwparserfromhell field extraction."""

from __future__ import annotations

from wiki_pipeline.infobox_parser import extract_infobox_fields

REQUIRED = ("birth_date", "death_date", "nationality", "occupation")


class TestExtractInfoboxFields:
    def test_full_infobox(self):
        wikitext = """{{Infobox person
| name = Claude Monet
| birth_date = {{birth date|1840|11|14}}
| death_date = {{death date|1926|12|5}}
| nationality = [[France|French]]
| occupation = [[Painting|Painter]]
}}"""
        result = extract_infobox_fields(wikitext, REQUIRED)
        assert result["birth_date"] == "1840-11-14"
        assert result["death_date"] == "1926-12-05"
        assert result["nationality"] == "French"
        assert result["occupation"] == "Painter"

    def test_partial_infobox(self):
        wikitext = """{{Infobox person
| name = Unknown
| birth_date = {{birth date|1900|1|1}}
}}"""
        result = extract_infobox_fields(wikitext, REQUIRED)
        assert result["birth_date"] == "1900-01-01"
        assert result["death_date"] is None
        assert result["nationality"] is None

    def test_no_infobox(self):
        wikitext = "'''Some Person''' was a notable figure."
        result = extract_infobox_fields(wikitext, REQUIRED)
        assert all(v is None for v in result.values())

    def test_artist_infobox_variant(self):
        wikitext = """{{Infobox artist
| name = Pablo Picasso
| birth_date = {{birth date|1881|10|25}}
| nationality = Spanish
| occupation = Painter, sculptor
}}"""
        result = extract_infobox_fields(wikitext, REQUIRED)
        assert result["birth_date"] == "1881-10-25"
        assert result["nationality"] == "Spanish"

    def test_painter_infobox_variant(self):
        wikitext = """{{Infobox painter
| born = {{birth date and age|1950|3|15}}
| nationality = Italian
}}"""
        result = extract_infobox_fields(wikitext, REQUIRED)
        assert result["birth_date"] == "1950-03-15"

    def test_birth_date_and_age_template(self):
        wikitext = """{{Infobox person
| birth_date = {{birth date and age|1985|6|20}}
}}"""
        result = extract_infobox_fields(wikitext, ("birth_date",))
        assert result["birth_date"] == "1985-06-20"

    def test_wikilink_stripping(self):
        wikitext = """{{Infobox person
| nationality = [[United Kingdom|British]]
| occupation = [[Actor]]
}}"""
        result = extract_infobox_fields(wikitext, ("nationality", "occupation"))
        assert result["nationality"] == "British"
        assert result["occupation"] == "Actor"

    def test_ref_stripping(self):
        wikitext = """{{Infobox person
| nationality = American<ref>Some source</ref>
}}"""
        result = extract_infobox_fields(wikitext, ("nationality",))
        assert result["nationality"] == "American"

    def test_field_aliases(self):
        """'born' alias resolves to birth_date field."""
        wikitext = """{{Infobox person
| born = {{birth date|1800|5|10}}
}}"""
        result = extract_infobox_fields(wikitext, ("birth_date",))
        assert result["birth_date"] == "1800-05-10"

    def test_empty_field_returns_none(self):
        wikitext = """{{Infobox person
| nationality =
}}"""
        result = extract_infobox_fields(wikitext, ("nationality",))
        assert result["nationality"] is None

    def test_royalty_infobox(self):
        # Plain-text dates are returned as-is by the infobox parser; the NLP
        # extractor normalises them in the full pipeline.
        wikitext = """{{Infobox royalty
| name         = Henry VIII
| birth_date   = 28 June 1491
| death_date   = 28 January 1547
| nationality  = English
| occupation   = King
}}"""
        result = extract_infobox_fields(wikitext, REQUIRED)
        assert result["birth_date"] is not None
        assert result["death_date"] is not None
        assert result["nationality"] == "English"
        assert result["occupation"] == "King"

    def test_monarch_infobox_template_dates(self):
        # {{birth date|...}} templates are resolved to ISO format.
        wikitext = """{{Infobox monarch
| name       = Charlemagne
| birth_date = {{birth date|748|4|2|df=y}}
| death_date = {{death date|814|1|28|df=y}}
}}"""
        result = extract_infobox_fields(wikitext, REQUIRED)
        assert result["birth_date"] == "748-04-02"
        assert result["death_date"] == "814-01-28"

    def test_noble_infobox(self):
        wikitext = """{{Infobox noble
| name       = William the Conqueror
| birth_date = c. 1028
| death_date = 9 September 1087
}}"""
        result = extract_infobox_fields(wikitext, REQUIRED)
        assert result["birth_date"] is not None
        assert result["death_date"] is not None

    def test_pharaoh_infobox(self):
        wikitext = """{{Infobox pharaoh
| name       = Ramesses II
| birth_date = c. 1303 BC
| death_date = 1213 BC
}}"""
        result = extract_infobox_fields(wikitext, REQUIRED)
        assert result["birth_date"] is not None
        assert result["death_date"] is not None
