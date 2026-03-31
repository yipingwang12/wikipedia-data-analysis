"""Tests for NLP-based text extractor — regex extraction from Wikipedia first sentences."""

from __future__ import annotations

from wiki_pipeline.nlp_extractor import extract_from_text

REQUIRED = ("birth_date", "death_date", "nationality", "occupation")
ALL_NONE = {f: None for f in REQUIRED}


# ---------------------------------------------------------------------------
# Interface contract
# ---------------------------------------------------------------------------

class TestInterfaceContract:
    def test_returns_copy_when_all_populated(self):
        existing = {
            "birth_date": "1840-11-14", "death_date": "1926-12-05",
            "nationality": "French", "occupation": "painter",
        }
        result = extract_from_text("Some text", existing, REQUIRED)
        assert result == existing
        assert result is not existing

    def test_only_fills_none_fields(self):
        text = "Claude Monet (14 November 1840 – 5 December 1926) was a French painter."
        existing = {
            "birth_date": "KEEP", "death_date": None,
            "nationality": None, "occupation": None,
        }
        result = extract_from_text(text, existing, REQUIRED)
        assert result["birth_date"] == "KEEP"

    def test_handles_empty_text(self):
        result = extract_from_text("", dict(ALL_NONE), REQUIRED)
        assert all(v is None for v in result.values())

    def test_handles_non_biographical_text(self):
        text = "The Eiffel Tower is a wrought-iron lattice tower in Paris."
        result = extract_from_text(text, dict(ALL_NONE), REQUIRED)
        assert result["nationality"] is None
        assert result["occupation"] is None

    def test_subset_of_required_fields(self):
        text = "Claude Monet (14 November 1840 – 5 December 1926) was a French painter."
        existing = {"birth_date": None, "nationality": None}
        result = extract_from_text(text, existing, ("birth_date", "nationality"))
        assert result["birth_date"] == "1840-11-14"
        assert result["nationality"] == "French"
        assert "death_date" not in result


# ---------------------------------------------------------------------------
# Date extraction
# ---------------------------------------------------------------------------

class TestDateExtraction:
    def test_standard_birth_death(self):
        text = "Claude Monet (14 November 1840 – 5 December 1926) was a French painter."
        result = extract_from_text(text, dict(ALL_NONE), REQUIRED)
        assert result["birth_date"] == "1840-11-14"
        assert result["death_date"] == "1926-12-05"

    def test_us_date_format(self):
        text = "John Smith (November 14, 1840 – December 5, 1926) was an American writer."
        result = extract_from_text(text, dict(ALL_NONE), REQUIRED)
        assert result["birth_date"] == "1840-11-14"
        assert result["death_date"] == "1926-12-05"

    def test_year_only(self):
        text = "Genghis Khan (1162–1227) was the founder of the Mongol Empire."
        result = extract_from_text(text, dict(ALL_NONE), REQUIRED)
        assert result["birth_date"] == "1162"
        assert result["death_date"] == "1227"

    def test_living_person_born(self):
        text = "Yo-Yo Ma (born October 7, 1955) is a French-born American cellist."
        result = extract_from_text(text, dict(ALL_NONE), REQUIRED)
        assert result["birth_date"] == "1955-10-07"
        assert result["death_date"] is None

    def test_circa_dates(self):
        text = "Leonardo da Vinci (c. 1452 – 1519) was an Italian polymath."
        result = extract_from_text(text, dict(ALL_NONE), REQUIRED)
        assert result["birth_date"] == "c. 1452"
        assert result["death_date"] == "1519"

    def test_hyphen_separator(self):
        text = "Jane Doe (1 January 1900 - 31 December 1999) was a British author."
        result = extract_from_text(text, dict(ALL_NONE), REQUIRED)
        assert result["birth_date"] == "1900-01-01"
        assert result["death_date"] == "1999-12-31"

    def test_aged_suffix(self):
        text = "Albert Einstein (14 March 1879 – 18 April 1955; aged 76) was a German-born theoretical physicist."
        result = extract_from_text(text, dict(ALL_NONE), REQUIRED)
        assert result["birth_date"] == "1879-03-14"
        assert result["death_date"] == "1955-04-18"

    def test_or_picks_earliest_birth(self):
        text = "Luigi De La Forest (Paris, 1668 or 1685 - Carpi, November 1, 1738) was an Italian painter."
        result = extract_from_text(text, dict(ALL_NONE), REQUIRED)
        assert result["birth_date"] == "1668"
        assert result["death_date"] == "1738-11-01"

    def test_or_picks_latest_death(self):
        text = "John Doe (1500 - 1570 or 1575) was an English writer."
        result = extract_from_text(text, dict(ALL_NONE), REQUIRED)
        assert result["birth_date"] == "1500"
        assert result["death_date"] == "1575"

    def test_parenthetical_with_places(self):
        text = "Giovanni da Bologna (Douai, 1529 – Florence, 13 August 1608) was a Flemish sculptor."
        result = extract_from_text(text, dict(ALL_NONE), REQUIRED)
        assert result["birth_date"] == "1529"
        assert result["death_date"] == "1608-08-13"

    def test_or_with_full_dates(self):
        text = "Jane Doe (1 January 1500 or 2 February 1505 – 1570) was a writer."
        result = extract_from_text(text, dict(ALL_NONE), REQUIRED)
        assert result["birth_date"] == "1500-01-01"
        assert result["death_date"] == "1570"

    def test_born_with_death_date(self):
        text = "Dieric Bouts (born c. 1415 – 6 May 1475) was an Early Netherlandish painter."
        result = extract_from_text(text, dict(ALL_NONE), REQUIRED)
        assert result["birth_date"] == "c. 1415"
        assert result["death_date"] == "1475-05-06"

    def test_no_parenthetical(self):
        text = "Confucius was a Chinese philosopher and politician."
        result = extract_from_text(text, dict(ALL_NONE), REQUIRED)
        assert result["birth_date"] is None
        assert result["death_date"] is None

    def test_day_month_year_format(self):
        text = "Frida Kahlo (6 July 1907 – 13 July 1954) was a Mexican painter."
        result = extract_from_text(text, dict(ALL_NONE), REQUIRED)
        assert result["birth_date"] == "1907-07-06"
        assert result["death_date"] == "1954-07-13"


# ---------------------------------------------------------------------------
# Nationality extraction
# ---------------------------------------------------------------------------

class TestNationalityExtraction:
    def test_single_nationality(self):
        text = "Claude Monet (14 November 1840 – 5 December 1926) was a French painter."
        result = extract_from_text(text, dict(ALL_NONE), REQUIRED)
        assert result["nationality"] == "French"

    def test_compound_and(self):
        text = "Marie Curie (7 November 1867 – 4 July 1934) was a Polish and naturalised-French physicist and chemist."
        result = extract_from_text(text, dict(ALL_NONE), REQUIRED)
        assert "Polish" in result["nationality"]
        assert "French" in result["nationality"]

    def test_hyphenated_born(self):
        text = "Albert Einstein (14 March 1879 – 18 April 1955) was a German-born theoretical physicist."
        result = extract_from_text(text, dict(ALL_NONE), REQUIRED)
        assert result["nationality"] == "German"

    def test_is_for_living_person(self):
        text = "Taylor Swift (born December 13, 1989) is an American singer-songwriter."
        result = extract_from_text(text, dict(ALL_NONE), REQUIRED)
        assert result["nationality"] == "American"

    def test_of_origin_pattern(self):
        text = "Charles van Beveren (1809–1850), was a painter of Belgian origin."
        result = extract_from_text(text, dict(ALL_NONE), REQUIRED)
        assert result["nationality"] == "Belgian"

    def test_of_descent_pattern(self):
        text = "John Smith (1900–1970) was an architect of Greek descent."
        result = extract_from_text(text, dict(ALL_NONE), REQUIRED)
        assert result["nationality"] == "Greek"

    def test_no_nationality_match(self):
        text = "The bridge (built 1890) was a structure in the city."
        result = extract_from_text(text, dict(ALL_NONE), REQUIRED)
        assert result["nationality"] is None

    def test_hyphenated_nationality(self):
        text = "Yo-Yo Ma (born October 7, 1955) is a French-born American cellist."
        result = extract_from_text(text, dict(ALL_NONE), REQUIRED)
        assert "American" in result["nationality"]


# ---------------------------------------------------------------------------
# Occupation extraction
# ---------------------------------------------------------------------------

class TestOccupationExtraction:
    def test_single_occupation(self):
        text = "Claude Monet (14 November 1840 – 5 December 1926) was a French painter."
        result = extract_from_text(text, dict(ALL_NONE), REQUIRED)
        assert result["occupation"] == "painter"

    def test_multiple_and(self):
        text = "Marie Curie (7 November 1867 – 4 July 1934) was a Polish and naturalised-French physicist and chemist."
        result = extract_from_text(text, dict(ALL_NONE), REQUIRED)
        assert "physicist" in result["occupation"]
        assert "chemist" in result["occupation"]

    def test_qualified_occupation(self):
        text = "Albert Einstein (14 March 1879 – 18 April 1955) was a German-born theoretical physicist."
        result = extract_from_text(text, dict(ALL_NONE), REQUIRED)
        assert "theoretical physicist" in result["occupation"]

    def test_singer_songwriter(self):
        text = "Bob Dylan (born May 24, 1941) is an American singer-songwriter."
        result = extract_from_text(text, dict(ALL_NONE), REQUIRED)
        assert "singer-songwriter" in result["occupation"] or "singer" in result["occupation"]

    def test_no_occupation_match(self):
        text = "The bridge was built in 1890."
        result = extract_from_text(text, dict(ALL_NONE), REQUIRED)
        assert result["occupation"] is None

    def test_comma_separated(self):
        text = "Leonardo da Vinci (15 April 1452 – 2 May 1519) was an Italian painter, sculptor, and architect."
        result = extract_from_text(text, dict(ALL_NONE), REQUIRED)
        assert "painter" in result["occupation"]
        assert "sculptor" in result["occupation"]
        assert "architect" in result["occupation"]


# ---------------------------------------------------------------------------
# Real Wikipedia examples (integration)
# ---------------------------------------------------------------------------

class TestRealWikipediaExamples:
    def test_claude_monet(self):
        text = (
            "Oscar-Claude Monet (14 November 1840 – 5 December 1926) was a French "
            "painter and founder of impressionist painting who is seen as a key "
            "precursor to modernism."
        )
        result = extract_from_text(text, dict(ALL_NONE), REQUIRED)
        assert result["birth_date"] == "1840-11-14"
        assert result["death_date"] == "1926-12-05"
        assert result["nationality"] == "French"
        assert "painter" in result["occupation"]

    def test_marie_curie(self):
        text = (
            "Marie Salomea Skłodowska-Curie (7 November 1867 – 4 July 1934) was a "
            "Polish and naturalised-French physicist and chemist who conducted "
            "pioneering research on radioactivity."
        )
        result = extract_from_text(text, dict(ALL_NONE), REQUIRED)
        assert result["birth_date"] == "1867-11-07"
        assert result["death_date"] == "1934-07-04"
        assert "Polish" in result["nationality"]
        assert "French" in result["nationality"]
        assert "physicist" in result["occupation"]
        assert "chemist" in result["occupation"]

    def test_albert_einstein(self):
        text = (
            "Albert Einstein (14 March 1879 – 18 April 1955) was a German-born "
            "theoretical physicist who is widely held to be one of the greatest "
            "and most influential scientists of all time."
        )
        result = extract_from_text(text, dict(ALL_NONE), REQUIRED)
        assert result["birth_date"] == "1879-03-14"
        assert result["death_date"] == "1955-04-18"
        assert result["nationality"] == "German"
        assert "theoretical physicist" in result["occupation"]

    def test_frida_kahlo(self):
        text = (
            "Magdalena Carmen Frida Kahlo y Calderón (6 July 1907 – 13 July 1954) "
            "was a Mexican painter known for her many portraits, self-portraits, "
            "and works inspired by the nature and artifacts of Mexico."
        )
        result = extract_from_text(text, dict(ALL_NONE), REQUIRED)
        assert result["birth_date"] == "1907-07-06"
        assert result["death_date"] == "1954-07-13"
        assert result["nationality"] == "Mexican"
        assert result["occupation"] == "painter"

    def test_living_person(self):
        text = (
            "Yo-Yo Ma (born October 7, 1955) is a French-born American cellist. "
            "Born in Paris, he was a child prodigy."
        )
        result = extract_from_text(text, dict(ALL_NONE), REQUIRED)
        assert result["birth_date"] == "1955-10-07"
        assert result["death_date"] is None
        assert "American" in result["nationality"]
        assert "cellist" in result["occupation"]

    def test_non_biographical(self):
        text = (
            "The Eiffel Tower is a wrought-iron lattice tower on the Champ de Mars "
            "in Paris, France. It is named after the engineer Gustave Eiffel."
        )
        result = extract_from_text(text, dict(ALL_NONE), REQUIRED)
        assert result["birth_date"] is None
        assert result["death_date"] is None
