"""Tests for output — CSV/TSV writer."""

from __future__ import annotations

from pathlib import Path


from wiki_pipeline.output import write_results


class TestWriteResults:
    def test_csv_output(self, tmp_path: Path):
        records = [
            {"page_id": 1, "title": "Monet", "birth_date": "1840-11-14", "nationality": "French"},
            {"page_id": 2, "title": "Renoir", "birth_date": "1841-02-25", "nationality": None},
        ]
        out = write_results(records, tmp_path / "out.csv", "csv")
        content = out.read_text()
        lines = content.strip().split("\n")
        assert lines[0] == "page_id,title,birth_date,nationality"
        assert "Monet" in lines[1]
        assert lines[2].endswith(",")  # None → empty string

    def test_tsv_output(self, tmp_path: Path):
        records = [{"page_id": 1, "title": "Test"}]
        out = write_results(records, tmp_path / "out.tsv", "tsv")
        content = out.read_text()
        assert "\t" in content

    def test_empty_records(self, tmp_path: Path):
        out = write_results([], tmp_path / "empty.csv", "csv")
        assert out.read_text() == ""

    def test_none_to_empty_string(self, tmp_path: Path):
        records = [{"a": None, "b": "val"}]
        out = write_results(records, tmp_path / "out.csv", "csv")
        lines = out.read_text().strip().split("\n")
        assert lines[1] == ",val"

    def test_creates_parent_dirs(self, tmp_path: Path):
        out = write_results(
            [{"x": 1}], tmp_path / "sub" / "dir" / "out.csv", "csv"
        )
        assert out.exists()

    def test_returns_path(self, tmp_path: Path):
        out = write_results([{"x": 1}], tmp_path / "out.csv", "csv")
        assert isinstance(out, Path)
        assert out == tmp_path / "out.csv"
