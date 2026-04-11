"""Tests for NLP-based text extractor — regex extraction from Wikipedia first sentences."""

from __future__ import annotations

from wiki_pipeline.nlp_extractor import extract_from_text, normalize_date, normalize_date_with_note

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


class TestNormalizeDate:
    def test_iso_passthrough(self):
        assert normalize_date("1840-11-14") == "1840-11-14"

    def test_year_only(self):
        assert normalize_date("1974") == "1974"

    def test_mdy(self):
        assert normalize_date("October 26, 1965") == "1965-10-26"

    def test_dmy(self):
        assert normalize_date("14 February 1989") == "1989-02-14"

    def test_dmy_with_comma(self):
        assert normalize_date("3 November,  2002") == "2002-11-03"

    def test_aged_suffix(self):
        assert normalize_date("August 25, 2001 (aged 22)") == "2001-08-25"

    def test_age_suffix(self):
        assert normalize_date("June 3, 2009 (age 72)") == "2009-06-03"

    def test_circa(self):
        assert normalize_date("c. 1450") == "c. 1450"

    def test_html_entities(self):
        assert normalize_date("14&nbsp;November&nbsp;1840") == "1840-11-14"

    def test_ordinals(self):
        assert normalize_date("3rd November 1840") == "1840-11-03"

    def test_month_year(self):
        assert normalize_date("November 1840") == "1840-11"

    def test_none(self):
        assert normalize_date(None) is None

    def test_empty(self):
        assert normalize_date("") is None

    def test_date_range(self):
        assert normalize_date("1840/42") == "1840"

    def test_numeric_dd_mm_yyyy(self):
        assert normalize_date("31-08-1983") == "1983-08-31"

    def test_numeric_dd_slash_mm_yyyy(self):
        assert normalize_date("31/08/1983") == "1983-08-31"

    def test_of_between_day_and_month(self):
        assert normalize_date("9 of May, 1971") == "1971-05-09"

    def test_of_between_day_and_month_no_space(self):
        assert normalize_date("3 of December,2001") == "2001-12-03"

    def test_mdy_no_space_after_comma(self):
        assert normalize_date("October 10,1985") == "1985-10-10"

    def test_on_prefix(self):
        assert normalize_date("On November 27, 1948") == "1948-11-27"

    def test_possibly_prefix_year(self):
        assert normalize_date("Possibly 1945") == "1945"

    def test_possibly_prefix_full_date(self):
        assert normalize_date("Possibly 17 July 1947 (aged 34) or 31 July 1952") == "1947-07-17"

    def test_died_prefix(self):
        assert normalize_date("Died February 4, 1983") == "1983-02-04"

    def test_birth_date_prefix(self):
        assert normalize_date("Birth date March 2, 1950") == "1950-03-02"

    def test_dot_separated_numeric(self):
        assert normalize_date("11.06.1958") == "1958-06-11"

    def test_or_alternatives_year(self):
        assert normalize_date("1608 or 1609") == "1608"

    def test_or_alternatives_full(self):
        assert normalize_date("1934 or 1935") == "1934"

    def test_bc_year(self):
        assert normalize_date("620 BC") == "620 BC"

    def test_bce_year(self):
        assert normalize_date("375 BCE") == "375 BC"

    def test_bc_mdy(self):
        assert normalize_date("March 20, 43 BC") == "43-03-20 BC"

    def test_bc_month_year(self):
        assert normalize_date("February 341 BC") == "341-02 BC"

    def test_bc_dmy(self):
        assert normalize_date("7 December 43 BC") == "43-12-07 BC"

    def test_incomplete_iso(self):
        assert normalize_date("1908-6-24") == "1908-06-24"

    def test_either_or(self):
        assert normalize_date("Either 1946, 1947 or 1949") == "1946"

    def test_year_comma_mdy(self):
        assert normalize_date("1955, January 19") == "1955"

    def test_template_junk_with_year(self):
        assert normalize_date("965}} ()}}") == "965"

    def test_template_junk_only(self):
        assert normalize_date("}}") is None

    def test_template_junk_with_aged(self):
        assert normalize_date("1040}} ()}} (aged around 75)") == "1040"


class TestNormalizeDateWithNote:
    def test_decade(self):
        assert normalize_date_with_note("1900s") == ("1900-01-01", "~1900s")

    def test_early_decade(self):
        assert normalize_date_with_note("early 1960s") == ("1960-01-01", "~early 1960s")

    def test_late_decade(self):
        assert normalize_date_with_note("late 1950s") == ("1950-01-01", "~late 1950s")

    def test_century(self):
        assert normalize_date_with_note("7 century") == ("0601-01-01", "~7 century")

    def test_century_with_ordinal(self):
        assert normalize_date_with_note("19th century") == ("1801-01-01", "~19th century")

    def test_century_hyphenated(self):
        assert normalize_date_with_note("19-century") == ("1801-01-01", "~19-century")

    def test_century_20(self):
        assert normalize_date_with_note("20-century") == ("1901-01-01", "~20-century")

    def test_regular_date_no_note(self):
        assert normalize_date_with_note("1840-11-14") == ("1840-11-14", None)

    def test_none(self):
        assert normalize_date_with_note(None) == (None, None)

    def test_month_day_mdy(self):
        assert normalize_date_with_note("January 10") == ("9999-01-10", "~January 10 (no year)")

    def test_month_day_dmy(self):
        assert normalize_date_with_note("19 November") == ("9999-11-19", "~19 November (no year)")

    def test_month_comma_day(self):
        assert normalize_date_with_note("November, 12") == ("9999-11-12", "~November, 12 (no year)")
