"""Tests for config — PipelineConfig dataclass + argparse CLI."""

from __future__ import annotations

from pathlib import Path

import pytest

from wiki_pipeline.config import PipelineConfig, parse_args, wiki_to_lang


class TestWikiToLang:
    def test_enwiki(self):
        assert wiki_to_lang("enwiki") == "en"

    def test_simplewiki(self):
        assert wiki_to_lang("simplewiki") == "simple"

    def test_frwiki(self):
        assert wiki_to_lang("frwiki") == "fr"

    def test_jawiki(self):
        assert wiki_to_lang("jawiki") == "ja"

    def test_no_wiki_suffix(self):
        assert wiki_to_lang("something") == "something"


class TestPipelineConfig:
    def test_defaults(self):
        cfg = PipelineConfig(root_category="Test")
        assert cfg.root_category == "Test"
        assert cfg.min_page_length == 5000
        assert cfg.output_format == "xlsx"
        assert cfg.dry_run is False
        assert cfg.no_cache is False
        assert cfg.clear_cache is False
        assert "birth_date" in cfg.required_fields

    def test_frozen(self):
        cfg = PipelineConfig(root_category="Test")
        with pytest.raises(AttributeError):
            cfg.root_category = "Other"

    def test_search_mode_bfs(self):
        cfg = PipelineConfig(root_category="Test")
        assert cfg.search_mode == "bfs"

    def test_search_mode_regex(self):
        cfg = PipelineConfig(category_patterns=("paint",))
        assert cfg.search_mode == "regex"

    def test_search_mode_regex_takes_precedence(self):
        cfg = PipelineConfig(root_category="Test", category_patterns=("paint",))
        assert cfg.search_mode == "regex"

    def test_extraction_mode_default_bio(self):
        cfg = PipelineConfig(root_category="Test")
        assert cfg.extraction_mode == "bio"

    def test_extraction_mode_geo(self):
        cfg = PipelineConfig(root_category="Test", extraction_mode="geo")
        assert cfg.extraction_mode == "geo"


class TestParseArgs:
    def test_minimal_bfs(self):
        cfg = parse_args(["Impressionist_painters"])
        assert cfg.root_category == "Impressionist_painters"
        assert cfg.min_page_length == 5000
        assert cfg.search_mode == "bfs"

    def test_all_flags(self):
        cfg = parse_args([
            "Cat",
            "--min-page-length", "1000",
            "--data-dir", "/tmp/data",
            "--results-dir", "/tmp/results",
            "--api-batch-size", "25",
            "--api-rate-limit", "0.5",
            "--claude-model", "custom-model",
            "--required-fields", "birth_date", "nationality",
            "--output-format", "tsv",
            "--dry-run",
        ])
        assert cfg.min_page_length == 1000
        assert cfg.data_dir == Path("/tmp/data")
        assert cfg.api_batch_size == 25
        assert cfg.api_rate_limit_s == 0.5
        assert cfg.required_fields == ("birth_date", "nationality")
        assert cfg.output_format == "tsv"
        assert cfg.dry_run is True

    def test_cache_flags(self):
        cfg = parse_args(["Cat", "--no-cache", "--clear-cache"])
        assert cfg.no_cache is True
        assert cfg.clear_cache is True

    def test_cache_flags_default_off(self):
        cfg = parse_args(["Cat"])
        assert cfg.no_cache is False
        assert cfg.clear_cache is False

    def test_no_cache_only(self):
        cfg = parse_args(["Cat", "--no-cache"])
        assert cfg.no_cache is True
        assert cfg.clear_cache is False

    def test_no_args_loads_all_bio_patterns(self):
        cfg = parse_args([])
        assert cfg.root_category is None
        assert len(cfg.category_patterns) > 0
        assert cfg.search_mode == "regex"

    def test_no_args_with_use_api_exits(self):
        with pytest.raises(SystemExit):
            parse_args(["--use-api"])

    def test_patterns_flag(self):
        cfg = parse_args(["--patterns", "French.*paint", "Italian.*sculpt"])
        assert cfg.category_patterns == ("French.*paint", "Italian.*sculpt")
        assert cfg.root_category is None
        assert cfg.search_mode == "regex"

    def test_patterns_file(self, tmp_path):
        pfile = tmp_path / "patterns.txt"
        pfile.write_text("French.*paint\nItalian.*sculpt\n\n")
        cfg = parse_args(["--patterns-file", str(pfile)])
        assert cfg.category_patterns == ("French.*paint", "Italian.*sculpt")

    def test_patterns_and_root_category(self):
        cfg = parse_args(["Root", "--patterns", "paint"])
        assert cfg.root_category == "Root"
        assert cfg.category_patterns == ("paint",)
        assert cfg.search_mode == "regex"

    def test_patterns_combined_with_file(self, tmp_path):
        pfile = tmp_path / "patterns.txt"
        pfile.write_text("from_file\n")
        cfg = parse_args(["--patterns", "from_cli", "--patterns-file", str(pfile)])
        assert cfg.category_patterns == ("from_cli", "from_file")

    def test_multistream_default_on(self):
        cfg = parse_args(["Cat"])
        assert cfg.use_multistream is True

    def test_no_multistream_flag(self):
        cfg = parse_args(["Cat", "--no-multistream"])
        assert cfg.use_multistream is False

    def test_wiki_default_enwiki(self):
        cfg = parse_args(["Cat"])
        assert cfg.wiki == "enwiki"
        assert "enwiki" in cfg.dump_base_url
        assert cfg.wiki_api_url == "https://en.wikipedia.org/w/api.php"

    def test_wiki_simplewiki(self):
        cfg = parse_args(["Cat", "--wiki", "simplewiki"])
        assert cfg.wiki == "simplewiki"
        assert "simplewiki" in cfg.dump_base_url
        assert cfg.wiki_api_url == "https://simple.wikipedia.org/w/api.php"

    def test_wiki_frwiki_api_url(self):
        cfg = parse_args(["Cat", "--wiki", "frwiki"])
        assert cfg.wiki == "frwiki"
        assert cfg.wiki_api_url == "https://fr.wikipedia.org/w/api.php"
        assert "frwiki" in cfg.dump_base_url

    def test_extraction_mode_auto_default(self):
        cfg = parse_args(["Cat"])
        assert cfg.extraction_mode == "auto"
        assert "birth_date" in cfg.required_fields

    def test_extraction_mode_bio_explicit(self):
        cfg = parse_args(["Cat", "--extraction-mode", "bio"])
        assert cfg.extraction_mode == "bio"
        assert "birth_date" in cfg.required_fields

    def test_extraction_mode_geo_default_fields(self):
        cfg = parse_args(["Cat", "--extraction-mode", "geo"])
        assert cfg.extraction_mode == "geo"
        assert "population" in cfg.required_fields
        assert "area_km2" in cfg.required_fields
        assert "birth_date" not in cfg.required_fields

    def test_extraction_mode_geo_with_custom_fields(self):
        cfg = parse_args(["Cat", "--extraction-mode", "geo", "--required-fields", "population"])
        assert cfg.extraction_mode == "geo"
        assert cfg.required_fields == ("population",)
