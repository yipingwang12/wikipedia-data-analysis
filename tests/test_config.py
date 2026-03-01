"""Tests for config — PipelineConfig dataclass + argparse CLI."""

from __future__ import annotations

from pathlib import Path

import pytest

from wiki_pipeline.config import PipelineConfig, parse_args


class TestPipelineConfig:
    def test_defaults(self):
        cfg = PipelineConfig(root_category="Test")
        assert cfg.root_category == "Test"
        assert cfg.min_page_length == 5000
        assert cfg.output_format == "csv"
        assert cfg.dry_run is False
        assert cfg.no_cache is False
        assert cfg.clear_cache is False
        assert "birth_date" in cfg.required_fields

    def test_frozen(self):
        cfg = PipelineConfig(root_category="Test")
        with pytest.raises(AttributeError):
            cfg.root_category = "Other"


class TestParseArgs:
    def test_minimal(self):
        cfg = parse_args(["Impressionist_painters"])
        assert cfg.root_category == "Impressionist_painters"
        assert cfg.min_page_length == 5000

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

    def test_missing_root_category_exits(self):
        with pytest.raises(SystemExit):
            parse_args([])
