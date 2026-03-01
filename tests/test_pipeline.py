"""Tests for pipeline orchestrator — end-to-end with mocked deps."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from wiki_pipeline.category_tree import CategoryTree, ParsedCategoryLinks
from wiki_pipeline.config import PipelineConfig
from wiki_pipeline.page_filter import PageInfo
from wiki_pipeline.pipeline import run


def _config(tmp_path: Path, **overrides) -> PipelineConfig:
    defaults = dict(
        root_category="Test_Category",
        data_dir=tmp_path / "data",
        results_dir=tmp_path / "results",
        min_page_length=100,
        dry_run=False,
        no_cache=True,
        clear_cache=False,
    )
    defaults.update(overrides)
    return PipelineConfig(**defaults)


_PARSED = ParsedCategoryLinks(
    children={"Test_Category": {"Sub"}},
    cat_pages={"Test_Category": {100}, "Sub": {101}},
)


class TestPipeline:
    @patch("wiki_pipeline.pipeline.download_dump")
    @patch("wiki_pipeline.pipeline.parse_page_dump")
    @patch("wiki_pipeline.pipeline.build_linktarget_map")
    @patch("wiki_pipeline.pipeline.parse_category_links")
    @patch("wiki_pipeline.pipeline.filter_pages_from_meta")
    @patch("wiki_pipeline.pipeline.WikiApiClient")
    @patch("wiki_pipeline.pipeline.LlmExtractor")
    @patch("wiki_pipeline.pipeline.load_dotenv")
    def test_end_to_end(
        self, mock_dotenv, mock_llm_cls, mock_api_cls,
        mock_filter, mock_catlinks, mock_lt, mock_page, mock_download, tmp_path
    ):
        mock_download.side_effect = lambda url, dest: dest
        mock_page.return_value = (
            {10: "Sub"},
            {100: ("Article_A", 6000), 101: ("Article_B", 7000)},
        )
        mock_lt.return_value = {500: "Test_Category"}
        mock_catlinks.return_value = _PARSED
        mock_filter.return_value = [
            PageInfo(100, "Article_A", 6000),
            PageInfo(101, "Article_B", 7000),
        ]
        api_instance = mock_api_cls.return_value
        api_instance.fetch_wikitext_batch.return_value = {
            "Article A": "{{Infobox person|birth_date={{birth date|1900|1|1}}|nationality=French|occupation=Painter|death_date={{death date|1970|1|1}}}}",
            "Article B": "No infobox here",
        }
        api_instance.fetch_plaintext_batch.return_value = {
            "Article A": "Article A text",
            "Article B": "Article B text",
        }
        llm_instance = mock_llm_cls.return_value
        llm_instance.extract_missing.return_value = {
            "birth_date": "1920", "death_date": None,
            "nationality": "Spanish", "occupation": "Sculptor",
        }

        config = _config(tmp_path)
        result = run(config)

        assert result is not None
        assert result.exists()
        content = result.read_text()
        assert "Article A" in content or "Article_A" in content

    @patch("wiki_pipeline.pipeline.download_dump")
    @patch("wiki_pipeline.pipeline.parse_page_dump")
    @patch("wiki_pipeline.pipeline.build_linktarget_map")
    @patch("wiki_pipeline.pipeline.parse_category_links")
    @patch("wiki_pipeline.pipeline.filter_pages_from_meta")
    @patch("wiki_pipeline.pipeline.load_dotenv")
    def test_dry_run(
        self, mock_dotenv, mock_filter, mock_catlinks, mock_lt,
        mock_page, mock_download, tmp_path
    ):
        mock_download.side_effect = lambda url, dest: dest
        mock_page.return_value = ({}, {1: ("A", 6000), 2: ("B", 7000)})
        mock_lt.return_value = {}
        mock_catlinks.return_value = ParsedCategoryLinks(
            cat_pages={"Test_Category": {1, 2, 3}},
        )
        mock_filter.return_value = [PageInfo(1, "A", 6000), PageInfo(2, "B", 7000)]

        config = _config(tmp_path, dry_run=True)
        result = run(config)
        assert result is None

    @patch("wiki_pipeline.pipeline.download_dump")
    @patch("wiki_pipeline.pipeline.parse_page_dump")
    @patch("wiki_pipeline.pipeline.build_linktarget_map")
    @patch("wiki_pipeline.pipeline.parse_category_links")
    @patch("wiki_pipeline.pipeline.filter_pages_from_meta")
    @patch("wiki_pipeline.pipeline.WikiApiClient")
    @patch("wiki_pipeline.pipeline.LlmExtractor")
    @patch("wiki_pipeline.pipeline.load_dotenv")
    def test_config_propagation(
        self, mock_dotenv, mock_llm_cls, mock_api_cls,
        mock_filter, mock_catlinks, mock_lt, mock_page, mock_download, tmp_path
    ):
        """API client receives config values."""
        mock_download.side_effect = lambda url, dest: dest
        mock_page.return_value = ({}, {1: ("A", 6000)})
        mock_lt.return_value = {}
        mock_catlinks.return_value = ParsedCategoryLinks(
            cat_pages={"Test_Category": {1}},
        )
        mock_filter.return_value = [PageInfo(1, "A", 6000)]
        api_instance = mock_api_cls.return_value
        api_instance.fetch_wikitext_batch.return_value = {"A": "text"}
        api_instance.fetch_plaintext_batch.return_value = {"A": "plain"}
        llm_instance = mock_llm_cls.return_value
        llm_instance.extract_missing.return_value = {
            "birth_date": None, "death_date": None,
            "nationality": None, "occupation": None,
        }

        config = _config(tmp_path, api_batch_size=25, api_rate_limit_s=0.5)
        run(config)

        mock_api_cls.assert_called_once_with(
            api_url=config.wiki_api_url,
            batch_size=25,
            rate_limit_s=0.5,
        )


class TestPipelineCache:
    @patch("wiki_pipeline.pipeline.download_dump")
    @patch("wiki_pipeline.pipeline.load_pickle")
    @patch("wiki_pipeline.pipeline.save_pickle")
    @patch("wiki_pipeline.pipeline.filter_pages_from_meta")
    @patch("wiki_pipeline.pipeline.load_dotenv")
    def test_cache_hit_skips_parsing(
        self, mock_dotenv, mock_filter, mock_save, mock_load, mock_download, tmp_path
    ):
        """When all 4 caches hit, no parse functions are called."""
        mock_download.side_effect = lambda url, dest: dest
        catid_map = {10: "Sub"}
        page_meta = {100: ("A", 6000)}
        lt_map = {500: "Test_Category"}
        parsed = _PARSED
        mock_load.side_effect = [catid_map, page_meta, lt_map, parsed]
        mock_filter.return_value = [PageInfo(100, "A", 6000)]

        config = _config(tmp_path, dry_run=True, no_cache=False)
        result = run(config)
        assert result is None
        mock_save.assert_not_called()

    @patch("wiki_pipeline.pipeline.download_dump")
    @patch("wiki_pipeline.pipeline.load_pickle")
    @patch("wiki_pipeline.pipeline.save_pickle")
    @patch("wiki_pipeline.pipeline.parse_page_dump")
    @patch("wiki_pipeline.pipeline.build_linktarget_map")
    @patch("wiki_pipeline.pipeline.parse_category_links")
    @patch("wiki_pipeline.pipeline.filter_pages_from_meta")
    @patch("wiki_pipeline.pipeline.load_dotenv")
    def test_cache_miss_parses_and_saves(
        self, mock_dotenv, mock_filter, mock_catlinks, mock_lt,
        mock_page, mock_save, mock_load, mock_download, tmp_path
    ):
        """On cache miss, all parse functions run and results are saved."""
        mock_download.side_effect = lambda url, dest: dest
        mock_load.return_value = None
        mock_page.return_value = ({10: "Sub"}, {100: ("A", 6000)})
        mock_lt.return_value = {500: "Test_Category"}
        mock_catlinks.return_value = _PARSED
        mock_filter.return_value = [PageInfo(100, "A", 6000)]

        config = _config(tmp_path, dry_run=True, no_cache=False)
        run(config)
        assert mock_save.call_count == 4
        mock_page.assert_called_once()

    @patch("wiki_pipeline.pipeline.download_dump")
    @patch("wiki_pipeline.pipeline.parse_page_dump")
    @patch("wiki_pipeline.pipeline.build_linktarget_map")
    @patch("wiki_pipeline.pipeline.parse_category_links")
    @patch("wiki_pipeline.pipeline.filter_pages_from_meta")
    @patch("wiki_pipeline.pipeline.clear_cache")
    @patch("wiki_pipeline.pipeline.load_dotenv")
    def test_clear_cache_flag(
        self, mock_dotenv, mock_clear, mock_filter, mock_catlinks,
        mock_lt, mock_page, mock_download, tmp_path
    ):
        mock_download.side_effect = lambda url, dest: dest
        mock_page.return_value = ({}, {})
        mock_lt.return_value = {}
        mock_catlinks.return_value = ParsedCategoryLinks()
        mock_filter.return_value = []
        mock_clear.return_value = 2

        config = _config(tmp_path, dry_run=True, clear_cache=True)
        run(config)
        mock_clear.assert_called_once_with(config.data_dir)

    @patch("wiki_pipeline.pipeline.download_dump")
    @patch("wiki_pipeline.pipeline.load_pickle")
    @patch("wiki_pipeline.pipeline.save_pickle")
    @patch("wiki_pipeline.pipeline.parse_page_dump")
    @patch("wiki_pipeline.pipeline.build_linktarget_map")
    @patch("wiki_pipeline.pipeline.parse_category_links")
    @patch("wiki_pipeline.pipeline.filter_pages_from_meta")
    @patch("wiki_pipeline.pipeline.load_dotenv")
    def test_no_cache_bypasses_load_and_save(
        self, mock_dotenv, mock_filter, mock_catlinks, mock_lt,
        mock_page, mock_save, mock_load, mock_download, tmp_path
    ):
        mock_download.side_effect = lambda url, dest: dest
        mock_page.return_value = ({}, {})
        mock_lt.return_value = {}
        mock_catlinks.return_value = ParsedCategoryLinks()
        mock_filter.return_value = []

        config = _config(tmp_path, dry_run=True, no_cache=True)
        run(config)
        mock_load.assert_not_called()
        mock_save.assert_not_called()
