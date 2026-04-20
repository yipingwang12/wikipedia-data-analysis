"""Tests for etymology_extractor."""

from __future__ import annotations

import pytest
from wiki_pipeline.etymology_extractor import (
    extract_etymology_section,
    extract_etymology_from_lead,
    extract_etymology_fields,
)


class TestExtractEtymologySection:
    def test_etymology_heading(self):
        wikitext = (
            "The Mississippi River is a river.\n\n"
            "== Etymology ==\n"
            "The name derives from the Ojibwe word ''misi-ziibi'' meaning 'Great River'.\n\n"
            "== Geography ==\n"
            "The river flows south.\n"
        )
        result = extract_etymology_section(wikitext)
        assert result is not None
        assert "Ojibwe" in result
        assert "Great River" in result

    def test_name_heading(self):
        wikitext = (
            "== Name ==\n"
            "The Colorado River takes its name from the Spanish word for 'red-colored'.\n"
        )
        result = extract_etymology_section(wikitext)
        assert result is not None
        assert "Spanish" in result

    def test_name_origin_heading(self):
        wikitext = (
            "== Name origin ==\n"
            "Named after the Platte River, meaning 'flat water' in French.\n"
        )
        result = extract_etymology_section(wikitext)
        assert result is not None
        assert "French" in result

    def test_history_and_name_heading(self):
        wikitext = (
            "== History and name ==\n"
            "The river was named by Spanish missionaries in 1769.\n"
        )
        result = extract_etymology_section(wikitext)
        assert result is not None
        assert "Spanish" in result

    def test_no_etymology_section(self):
        wikitext = (
            "== Geography ==\n"
            "The river flows through several states.\n\n"
            "== History ==\n"
            "First explored in 1804.\n"
        )
        assert extract_etymology_section(wikitext) is None

    def test_empty_wikitext(self):
        assert extract_etymology_section("") is None

    def test_case_insensitive_heading(self):
        wikitext = "== ETYMOLOGY ==\nFrom Latin ''flumen''.\n"
        result = extract_etymology_section(wikitext)
        assert result is not None

    def test_level3_heading(self):
        wikitext = (
            "== History ==\n"
            "Long history.\n"
            "=== Etymology ===\n"
            "Named by the French explorers.\n"
        )
        result = extract_etymology_section(wikitext)
        assert result is not None
        assert "French" in result

    def test_truncates_at_2000_chars(self):
        wikitext = f"== Etymology ==\n{'A' * 3000}\n"
        result = extract_etymology_section(wikitext)
        assert result is not None
        assert len(result) <= 2000

    def test_heading_text_excluded_from_result(self):
        wikitext = "== Etymology ==\nDerives from Latin.\n"
        result = extract_etymology_section(wikitext)
        assert result is not None
        assert "==" not in result

    def test_derivation_heading(self):
        wikitext = "== Derivation of name ==\nFrom the indigenous word for 'clear water'.\n"
        result = extract_etymology_section(wikitext)
        assert result is not None

    def test_naming_heading(self):
        wikitext = "== Naming ==\nThe peak was named after President Lincoln in 1871.\n"
        result = extract_etymology_section(wikitext)
        assert result is not None


class TestExtractEtymologyFromLead:
    def test_named_after(self):
        plaintext = (
            "The Hudson River is a river in New York. "
            "It is named after Henry Hudson, the English explorer who sailed it in 1609. "
            "The river flows south into Upper New York Bay."
        )
        result = extract_etymology_from_lead(plaintext)
        assert result is not None
        assert "Hudson" in result

    def test_name_derives_from(self):
        plaintext = (
            "The Ohio River is a major river. "
            "The name derives from the Seneca word 'ohiiyo' meaning 'great river'. "
            "It forms the boundary between Ohio and Kentucky."
        )
        result = extract_etymology_from_lead(plaintext)
        assert result is not None
        assert "Seneca" in result

    def test_name_means(self):
        plaintext = (
            "The Platte River is a river in Nebraska. "
            "Its name means 'flat' in French, referring to the shallow, wide channel. "
            "The river drains a large area of the Great Plains."
        )
        result = extract_etymology_from_lead(plaintext)
        assert result is not None
        assert "flat" in result.lower()

    def test_named_in_honor(self):
        plaintext = (
            "Mount Rainier is a volcano. "
            "It was named in honor of Rear Admiral Peter Rainier by George Vancouver in 1792. "
            "The mountain is the highest in Washington state."
        )
        result = extract_etymology_from_lead(plaintext)
        assert result is not None

    def test_no_etymology_in_lead(self):
        plaintext = (
            "The Arkansas River is a tributary of the Mississippi River. "
            "It originates in the Rocky Mountains of Colorado. "
            "It passes through Kansas and Oklahoma before reaching Arkansas."
        )
        assert extract_etymology_from_lead(plaintext) is None

    def test_empty_plaintext(self):
        assert extract_etymology_from_lead("") is None

    def test_only_searches_lead(self):
        # Etymology mention is beyond the 1500-char lead window
        lead = "The river is long. " * 80  # ~1520 chars, beyond window
        etymology = "It is named after General Sherman."
        plaintext = lead + etymology
        result = extract_etymology_from_lead(plaintext)
        assert result is None

    def test_short_match_excluded(self):
        # Very short sentence containing etymology keyword — should be excluded
        plaintext = "Named. Some other longer text about the geography of this place."
        result = extract_etymology_from_lead(plaintext)
        # Either None or something substantial — not a one-word fragment
        if result is not None:
            assert len(result) > 20


class TestExtractEtymologyFields:
    def test_returns_etymology_key(self):
        wikitext = "== Etymology ==\nFrom the Ojibwe word meaning 'big river'.\n"
        result = extract_etymology_fields(wikitext)
        assert "etymology" in result
        assert result["etymology"] is not None
        assert "Ojibwe" in result["etymology"]

    def test_none_when_no_section(self):
        result = extract_etymology_fields("No etymology section here.")
        assert result == {"etymology": None}

    def test_empty_wikitext(self):
        result = extract_etymology_fields("")
        assert result == {"etymology": None}

    def test_custom_required_fields_ignored(self):
        # extra fields in required_fields are not in output — only etymology is produced
        wikitext = "== Etymology ==\nFrom Latin.\n"
        result = extract_etymology_fields(wikitext, required_fields=("etymology",))
        assert set(result.keys()) == {"etymology"}
