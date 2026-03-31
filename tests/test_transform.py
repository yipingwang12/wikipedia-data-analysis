"""Tests for transform_to_gadm — CSV → wikipedia.json with GADM join."""

from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from scripts.transform_to_gadm import build_gadm_index, normalize, transform


@pytest.fixture
def gadm_dir(tmp_path):
    """Create a minimal GADM data directory with region GeoJSON files."""
    regions = tmp_path / "regions"
    regions.mkdir()

    illinois = {
        "type": "FeatureCollection",
        "features": [
            {"type": "Feature", "properties": {
                "GID_2": "USA.14.1_1", "NAME_2": "Adams", "NAME_1": "Illinois",
                "VARNAME_2": "NA", "TYPE_2": "County",
            }, "geometry": {"type": "Point", "coordinates": [0, 0]}},
            {"type": "Feature", "properties": {
                "GID_2": "USA.14.17_1", "NAME_2": "Cook", "NAME_1": "Illinois",
                "VARNAME_2": "NA", "TYPE_2": "County",
            }, "geometry": {"type": "Point", "coordinates": [0, 0]}},
        ],
    }
    (regions / "illinois.json").write_text(json.dumps(illinois))

    ohio = {
        "type": "FeatureCollection",
        "features": [
            {"type": "Feature", "properties": {
                "GID_2": "USA.36.1_1", "NAME_2": "Adams", "NAME_1": "Ohio",
                "VARNAME_2": "NA", "TYPE_2": "County",
            }, "geometry": {"type": "Point", "coordinates": [0, 0]}},
        ],
    }
    (regions / "ohio.json").write_text(json.dumps(ohio))
    return tmp_path


@pytest.fixture
def csv_path(tmp_path):
    """Create a sample pipeline CSV output."""
    path = tmp_path / "geo_results.csv"
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["page_id", "title", "population", "area_km2"])
        w.writeheader()
        w.writerow({"page_id": "1", "title": "Cook County, Illinois", "population": "5275541", "area_km2": "2448"})
        w.writerow({"page_id": "2", "title": "Adams County, Ohio", "population": "27926", "area_km2": "1522"})
        w.writerow({"page_id": "3", "title": "Adams County, Illinois", "population": "65435", "area_km2": "2211"})
    return path


class TestNormalize:
    def test_basic(self):
        assert normalize("Cook County, Illinois") == "cook, illinois"

    def test_strips_county_suffix(self):
        assert normalize("Cook County") == "cook"

    def test_strips_parish_suffix(self):
        assert normalize("East Baton Rouge Parish") == "east baton rouge"

    def test_strips_borough_suffix(self):
        assert normalize("Fairbanks North Star Borough") == "fairbanks north star"

    def test_preserves_comma_state(self):
        assert normalize("Cook County, Illinois") == "cook, illinois"

    def test_collapses_whitespace(self):
        assert normalize("  Cook   County  ") == "cook"

    def test_underscores_to_spaces(self):
        assert normalize("Cook_County,_Illinois") == "cook, illinois"


class TestBuildGadmIndex:
    def test_builds_index(self, gadm_dir):
        index = build_gadm_index(gadm_dir)
        assert index["cook, illinois"] == "USA.14.17_1"
        assert index["adams, illinois"] == "USA.14.1_1"
        assert index["adams, ohio"] == "USA.36.1_1"

    def test_empty_dir(self, tmp_path):
        (tmp_path / "regions").mkdir()
        index = build_gadm_index(tmp_path)
        assert index == {}


class TestTransform:
    def test_full_transform(self, csv_path, gadm_dir, tmp_path):
        output = tmp_path / "wikipedia.json"
        transform(csv_path, gadm_dir, output)

        data = json.loads(output.read_text())
        assert "USA.14.17_1" in data
        assert data["USA.14.17_1"]["title"] == "Cook County, Illinois"
        assert data["USA.14.17_1"]["population"] == "5275541"
        assert "wikipedia_url" in data["USA.14.17_1"]

    def test_all_rows_matched(self, csv_path, gadm_dir, tmp_path):
        output = tmp_path / "wikipedia.json"
        transform(csv_path, gadm_dir, output)

        data = json.loads(output.read_text())
        assert len(data) == 3  # Cook IL, Adams OH, Adams IL

    def test_unmatched_logged(self, gadm_dir, tmp_path):
        csv_file = tmp_path / "input.csv"
        with open(csv_file, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["page_id", "title", "population"])
            w.writeheader()
            w.writerow({"page_id": "1", "title": "Nonexistent County, Nowhere", "population": "100"})

        output = tmp_path / "wikipedia.json"
        transform(csv_file, gadm_dir, output)

        data = json.loads(output.read_text())
        assert len(data) == 0

        unmatched = tmp_path / "unmatched.csv"
        assert unmatched.exists()
        content = unmatched.read_text()
        assert "Nonexistent" in content

    def test_wikipedia_url_generated(self, csv_path, gadm_dir, tmp_path):
        output = tmp_path / "wikipedia.json"
        transform(csv_path, gadm_dir, output)

        data = json.loads(output.read_text())
        entry = data["USA.14.17_1"]
        assert entry["wikipedia_url"] == "https://en.wikipedia.org/wiki/Cook_County,_Illinois"
