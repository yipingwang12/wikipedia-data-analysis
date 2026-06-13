"""Tests for scripts/run_geo_integration.py."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch, MagicMock
import sys


# Ensure scripts/ is importable
_SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import run_geo_integration as rgi


class TestRunPipelineCmd:
    """M4 regression: run_pipeline must pass --output-format csv so the glob finds a CSV."""

    def test_cmd_includes_output_format_csv(self, tmp_path):
        """The subprocess command must contain --output-format csv."""
        captured = {}

        def fake_run(cmd, **kwargs):
            captured["cmd"] = cmd
            result = MagicMock()
            result.returncode = 0
            return result

        patterns_file = tmp_path / "rivers.txt"
        patterns_file.write_text("river\n")
        results_dir = tmp_path / "results"
        results_dir.mkdir()
        # Create a CSV so the glob finds it
        (results_dir / "rivers.csv").write_text("page_id,title\n1,Foo\n")

        with patch("run_geo_integration.subprocess.run", side_effect=fake_run):
            rgi.run_pipeline(patterns_file, results_dir)

        assert "--output-format" in captured["cmd"]
        idx = captured["cmd"].index("--output-format")
        assert captured["cmd"][idx + 1] == "csv"

    def test_glob_finds_csv_after_pipeline(self, tmp_path):
        """run_pipeline returns a Path when a CSV exists in results_dir."""
        def fake_run(cmd, **kwargs):
            result = MagicMock()
            result.returncode = 0
            return result

        patterns_file = tmp_path / "rivers.txt"
        patterns_file.write_text("river\n")
        results_dir = tmp_path / "results"
        results_dir.mkdir()
        (results_dir / "rivers.csv").write_text("page_id,title\n1,Foo\n")

        with patch("run_geo_integration.subprocess.run", side_effect=fake_run):
            csv_path = rgi.run_pipeline(patterns_file, results_dir)

        assert csv_path is not None
        assert csv_path.suffix == ".csv"

    def test_glob_finds_nothing_returns_none(self, tmp_path):
        """run_pipeline returns None when no CSV exists (pipeline wrote xlsx instead)."""
        def fake_run(cmd, **kwargs):
            result = MagicMock()
            result.returncode = 0
            return result

        patterns_file = tmp_path / "rivers.txt"
        patterns_file.write_text("river\n")
        results_dir = tmp_path / "results"
        results_dir.mkdir()
        # No CSV — only xlsx (the pre-fix behaviour)
        (results_dir / "rivers.xlsx").write_text("")

        with patch("run_geo_integration.subprocess.run", side_effect=fake_run):
            csv_path = rgi.run_pipeline(patterns_file, results_dir)

        assert csv_path is None
