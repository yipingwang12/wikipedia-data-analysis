"""Tests for pipeline orchestrator — end-to-end with mocked deps."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import openpyxl

from wiki_pipeline.category_tree import CategoryTree, ParsedCategoryLinks
from wiki_pipeline.config import PipelineConfig
from wiki_pipeline.page_filter import PageInfo
from wiki_pipeline.pipeline import run


def _read_xlsx_text(path: Path) -> str:
    """Read xlsx file and return all cell values as a single string for assertion checks."""
    wb = openpyxl.load_workbook(path)
    ws = wb.active
    return " ".join(str(cell.value or "") for row in ws.iter_rows() for cell in row)


def _config(tmp_path: Path, **overrides) -> PipelineConfig:
    defaults = dict(
        root_category="Test_Category",
        data_dir=tmp_path / "data",
        results_dir=tmp_path / "results",
        min_page_length=100,
        dry_run=False,
        no_cache=True,
        clear_cache=False,
        use_api=True,
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
    @patch("wiki_pipeline.pipeline.extract_from_text")
    @patch("wiki_pipeline.pipeline.load_dotenv")
    def test_end_to_end(
        self, mock_dotenv, mock_nlp, mock_llm_cls, mock_api_cls,
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
        # NLP passthrough (returns all None → LLM still called)
        mock_nlp.side_effect = lambda text, existing, fields: dict(existing)
        llm_instance = mock_llm_cls.return_value
        llm_instance.extract_missing.return_value = {
            "birth_date": "1920", "death_date": None,
            "nationality": "Spanish", "occupation": "Sculptor",
        }

        config = _config(tmp_path)
        result = run(config)

        assert result is not None
        assert result.exists()
        content = _read_xlsx_text(result)
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
    @patch("wiki_pipeline.pipeline.extract_from_text")
    @patch("wiki_pipeline.pipeline.load_dotenv")
    def test_config_propagation(
        self, mock_dotenv, mock_nlp, mock_llm_cls, mock_api_cls,
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
        mock_nlp.side_effect = lambda text, existing, fields: dict(existing)
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


    @patch("wiki_pipeline.pipeline.download_dump")
    @patch("wiki_pipeline.pipeline.parse_page_dump")
    @patch("wiki_pipeline.pipeline.build_linktarget_map")
    @patch("wiki_pipeline.pipeline.parse_category_links")
    @patch("wiki_pipeline.pipeline.bfs_from_root")
    @patch("wiki_pipeline.pipeline.filter_pages_from_meta")
    @patch("wiki_pipeline.pipeline.load_dotenv")
    def test_max_depth_passed_to_bfs(
        self, mock_dotenv, mock_filter, mock_bfs, mock_catlinks, mock_lt,
        mock_page, mock_download, tmp_path
    ):
        mock_download.side_effect = lambda url, dest: dest
        mock_page.return_value = ({}, {1: ("A", 6000)})
        mock_lt.return_value = {}
        mock_catlinks.return_value = _PARSED
        mock_bfs.return_value = CategoryTree(
            article_ids={1}, depth_stats=[(1, 1)],
        )
        mock_filter.return_value = [PageInfo(1, "A", 6000)]

        config = _config(tmp_path, dry_run=True, max_depth=5)
        run(config)
        mock_bfs.assert_called_once_with(_PARSED, "Test_Category", max_depth=5)


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
        mock_clear.assert_called_once_with(config.data_dir, config.wiki)

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


class TestPipelineRegexMode:
    @patch("wiki_pipeline.pipeline.download_dump")
    @patch("wiki_pipeline.pipeline.parse_page_dump")
    @patch("wiki_pipeline.pipeline.build_linktarget_map")
    @patch("wiki_pipeline.pipeline.parse_category_links")
    @patch("wiki_pipeline.pipeline.filter_pages_from_meta")
    @patch("wiki_pipeline.pipeline.load_dotenv")
    def test_regex_dry_run(
        self, mock_dotenv, mock_filter, mock_catlinks, mock_lt,
        mock_page, mock_download, tmp_path
    ):
        mock_download.side_effect = lambda url, dest: dest
        mock_page.return_value = (
            {1: "French_painters", 2: "Italian_sculptors", 3: "German_poets"},
            {100: ("Monet", 6000), 101: ("Rodin", 7000), 102: ("Goethe", 5000)},
        )
        mock_lt.return_value = {}
        mock_catlinks.return_value = ParsedCategoryLinks(
            cat_pages={
                "French_painters": {100},
                "Italian_sculptors": {101},
                "German_poets": {102},
            },
        )
        mock_filter.return_value = [PageInfo(100, "Monet", 6000)]

        config = _config(
            tmp_path,
            root_category=None,
            category_patterns=(r"French",),
            dry_run=True,
        )
        result = run(config)
        assert result is None
        # filter_pages_from_meta should have been called with only French_painters articles
        call_args = mock_filter.call_args
        assert call_args[0][1] == {100}  # article_ids from French_painters only

    @patch("wiki_pipeline.pipeline.download_dump")
    @patch("wiki_pipeline.pipeline.parse_page_dump")
    @patch("wiki_pipeline.pipeline.build_linktarget_map")
    @patch("wiki_pipeline.pipeline.parse_category_links")
    @patch("wiki_pipeline.pipeline.filter_pages_from_meta")
    @patch("wiki_pipeline.pipeline.load_dotenv")
    def test_regex_multiple_patterns(
        self, mock_dotenv, mock_filter, mock_catlinks, mock_lt,
        mock_page, mock_download, tmp_path
    ):
        mock_download.side_effect = lambda url, dest: dest
        mock_page.return_value = (
            {1: "French_painters", 2: "Italian_sculptors", 3: "German_poets"},
            {100: ("A", 6000), 101: ("B", 7000), 102: ("C", 5000)},
        )
        mock_lt.return_value = {}
        mock_catlinks.return_value = ParsedCategoryLinks(
            cat_pages={
                "French_painters": {100},
                "Italian_sculptors": {101},
                "German_poets": {102},
            },
        )
        mock_filter.return_value = []

        config = _config(
            tmp_path,
            root_category=None,
            category_patterns=(r"paint", r"sculpt"),
            dry_run=True,
        )
        run(config)
        call_args = mock_filter.call_args
        assert call_args[0][1] == {100, 101}

    @patch("wiki_pipeline.pipeline.download_dump")
    @patch("wiki_pipeline.pipeline.parse_page_dump")
    @patch("wiki_pipeline.pipeline.build_linktarget_map")
    @patch("wiki_pipeline.pipeline.parse_category_links")
    @patch("wiki_pipeline.pipeline.filter_pages_from_meta")
    @patch("wiki_pipeline.pipeline.load_dotenv")
    def test_regex_no_bfs_called(
        self, mock_dotenv, mock_filter, mock_catlinks, mock_lt,
        mock_page, mock_download, tmp_path
    ):
        """Regex mode should not call bfs_from_root."""
        mock_download.side_effect = lambda url, dest: dest
        mock_page.return_value = ({1: "CatA"}, {})
        mock_lt.return_value = {}
        mock_catlinks.return_value = ParsedCategoryLinks(cat_pages={"CatA": {1}})
        mock_filter.return_value = []

        config = _config(
            tmp_path,
            root_category=None,
            category_patterns=(r"Cat",),
            dry_run=True,
        )
        with patch("wiki_pipeline.pipeline.bfs_from_root") as mock_bfs:
            run(config)
            mock_bfs.assert_not_called()

    @patch("wiki_pipeline.pipeline.download_dump")
    @patch("wiki_pipeline.pipeline.parse_page_dump")
    @patch("wiki_pipeline.pipeline.build_linktarget_map")
    @patch("wiki_pipeline.pipeline.parse_category_links")
    @patch("wiki_pipeline.pipeline.filter_pages_from_meta")
    @patch("wiki_pipeline.pipeline.WikiApiClient")
    @patch("wiki_pipeline.pipeline.LlmExtractor")
    @patch("wiki_pipeline.pipeline.extract_from_text")
    @patch("wiki_pipeline.pipeline.load_dotenv")
    def test_regex_full_run(
        self, mock_dotenv, mock_nlp, mock_llm_cls, mock_api_cls,
        mock_filter, mock_catlinks, mock_lt, mock_page, mock_download, tmp_path
    ):
        mock_download.side_effect = lambda url, dest: dest
        mock_page.return_value = (
            {1: "French_painters"},
            {100: ("Monet", 6000)},
        )
        mock_lt.return_value = {}
        mock_catlinks.return_value = ParsedCategoryLinks(
            cat_pages={"French_painters": {100}},
        )
        mock_filter.return_value = [PageInfo(100, "Monet", 6000)]
        api_instance = mock_api_cls.return_value
        api_instance.fetch_wikitext_batch.return_value = {"Monet": "{{Infobox}}"}
        api_instance.fetch_plaintext_batch.return_value = {"Monet": "text"}
        mock_nlp.side_effect = lambda text, existing, fields: dict(existing)
        llm_instance = mock_llm_cls.return_value
        llm_instance.extract_missing.return_value = {
            "birth_date": "1840", "death_date": "1926",
            "nationality": "French", "occupation": "Painter",
        }

        config = _config(
            tmp_path,
            root_category=None,
            category_patterns=(r"French",),
        )
        result = run(config)
        assert result is not None
        assert result.exists()
        assert "Monet" in _read_xlsx_text(result)


class TestPipelineGeoMode:
    @patch("wiki_pipeline.pipeline.download_dump")
    @patch("wiki_pipeline.pipeline.parse_page_dump")
    @patch("wiki_pipeline.pipeline.build_linktarget_map")
    @patch("wiki_pipeline.pipeline.parse_category_links")
    @patch("wiki_pipeline.pipeline.filter_pages_from_meta")
    @patch("wiki_pipeline.pipeline.WikiApiClient")
    @patch("wiki_pipeline.pipeline.LlmExtractor")
    @patch("wiki_pipeline.pipeline.extract_from_text")
    @patch("wiki_pipeline.pipeline.load_dotenv")
    def test_geo_mode_uses_geo_extractor(
        self, mock_dotenv, mock_nlp, mock_llm_cls, mock_api_cls,
        mock_filter, mock_catlinks, mock_lt, mock_page, mock_download, tmp_path
    ):
        """Geo mode should use extract_geo_infobox_fields, not extract_infobox_fields."""
        mock_download.side_effect = lambda url, dest: dest
        mock_page.return_value = ({}, {1: ("A", 6000)})
        mock_lt.return_value = {}
        mock_catlinks.return_value = ParsedCategoryLinks(
            cat_pages={"Test_Category": {1}},
        )
        mock_filter.return_value = [PageInfo(1, "A", 6000)]
        api = mock_api_cls.return_value
        api.fetch_wikitext_batch.return_value = {
            "A": "{{Infobox settlement|population_total=50000|area_km2=100|elevation_m=200|subdivision_name1=Illinois|subdivision_type1=State}}",
        }
        api.fetch_plaintext_batch.return_value = {"A": "Springfield is a city."}
        mock_nlp.side_effect = lambda text, existing, fields: dict(existing)
        mock_llm_cls.return_value.extract_missing.return_value = {
            "population": "50000", "area_km2": "100",
            "elevation_m": "200", "subdivision_name": "Illinois",
            "subdivision_type": "State",
        }

        geo_fields = ("population", "area_km2", "elevation_m", "subdivision_name", "subdivision_type")
        config = _config(tmp_path, extraction_mode="geo", required_fields=geo_fields)
        result = run(config)

        assert result is not None
        assert result.exists()
        content = _read_xlsx_text(result)
        assert "50000" in content
        assert "Illinois" in content

    @patch("wiki_pipeline.pipeline.download_dump")
    @patch("wiki_pipeline.pipeline.parse_page_dump")
    @patch("wiki_pipeline.pipeline.build_linktarget_map")
    @patch("wiki_pipeline.pipeline.parse_category_links")
    @patch("wiki_pipeline.pipeline.filter_pages_from_meta")
    @patch("wiki_pipeline.pipeline.load_dotenv")
    def test_geo_mode_dry_run(
        self, mock_dotenv, mock_filter, mock_catlinks, mock_lt,
        mock_page, mock_download, tmp_path
    ):
        mock_download.side_effect = lambda url, dest: dest
        mock_page.return_value = ({1: "Counties_in_Illinois"}, {100: ("Cook_County", 6000)})
        mock_lt.return_value = {}
        mock_catlinks.return_value = ParsedCategoryLinks(
            cat_pages={"Counties_in_Illinois": {100}},
        )
        mock_filter.return_value = [PageInfo(100, "Cook_County", 6000)]

        config = _config(
            tmp_path,
            root_category=None,
            category_patterns=(r"Counties(?:_in_|_of_)",),
            extraction_mode="geo",
            required_fields=("population", "area_km2"),
            dry_run=True,
        )
        result = run(config)
        assert result is None


class TestPipelineThreeTierExtraction:
    """Verify infobox → NLP → LLM fallback chain."""

    def _setup_mocks(self, tmp_path, mock_download, mock_page, mock_lt,
                     mock_catlinks, mock_filter, mock_api_cls):
        mock_download.side_effect = lambda url, dest: dest
        mock_page.return_value = ({}, {1: ("A", 6000)})
        mock_lt.return_value = {}
        mock_catlinks.return_value = ParsedCategoryLinks(
            cat_pages={"Test_Category": {1}},
        )
        mock_filter.return_value = [PageInfo(1, "A", 6000)]
        api = mock_api_cls.return_value
        api.fetch_wikitext_batch.return_value = {"A": "no infobox"}
        api.fetch_plaintext_batch.return_value = {"A": "some text"}
        return api

    @patch("wiki_pipeline.pipeline.download_dump")
    @patch("wiki_pipeline.pipeline.parse_page_dump")
    @patch("wiki_pipeline.pipeline.build_linktarget_map")
    @patch("wiki_pipeline.pipeline.parse_category_links")
    @patch("wiki_pipeline.pipeline.filter_pages_from_meta")
    @patch("wiki_pipeline.pipeline.WikiApiClient")
    @patch("wiki_pipeline.pipeline.LlmExtractor")
    @patch("wiki_pipeline.pipeline.extract_from_text")
    @patch("wiki_pipeline.pipeline.load_dotenv")
    def test_nlp_fills_all_gaps_skips_llm(
        self, mock_dotenv, mock_nlp, mock_llm_cls, mock_api_cls,
        mock_filter, mock_catlinks, mock_lt, mock_page, mock_download, tmp_path
    ):
        """When NLP fills all gaps, LLM should not be called."""
        self._setup_mocks(tmp_path, mock_download, mock_page, mock_lt,
                          mock_catlinks, mock_filter, mock_api_cls)
        mock_nlp.return_value = {
            "birth_date": "1900-01-01", "death_date": "1970-01-01",
            "nationality": "French", "occupation": "painter",
        }

        config = _config(tmp_path)
        run(config)

        mock_nlp.assert_called_once()
        mock_llm_cls.return_value.extract_missing.assert_not_called()

    @patch("wiki_pipeline.pipeline.download_dump")
    @patch("wiki_pipeline.pipeline.parse_page_dump")
    @patch("wiki_pipeline.pipeline.build_linktarget_map")
    @patch("wiki_pipeline.pipeline.parse_category_links")
    @patch("wiki_pipeline.pipeline.filter_pages_from_meta")
    @patch("wiki_pipeline.pipeline.WikiApiClient")
    @patch("wiki_pipeline.pipeline.LlmExtractor")
    @patch("wiki_pipeline.pipeline.extract_from_text")
    @patch("wiki_pipeline.pipeline.load_dotenv")
    def test_nlp_partial_fill_then_llm(
        self, mock_dotenv, mock_nlp, mock_llm_cls, mock_api_cls,
        mock_filter, mock_catlinks, mock_lt, mock_page, mock_download, tmp_path
    ):
        """When NLP fills some gaps, LLM is called for remaining."""
        self._setup_mocks(tmp_path, mock_download, mock_page, mock_lt,
                          mock_catlinks, mock_filter, mock_api_cls)
        mock_nlp.return_value = {
            "birth_date": "1900-01-01", "death_date": "1970-01-01",
            "nationality": None, "occupation": None,
        }
        mock_llm_cls.return_value.extract_missing.return_value = {
            "birth_date": "1900-01-01", "death_date": "1970-01-01",
            "nationality": "French", "occupation": "painter",
        }

        config = _config(tmp_path)
        run(config)

        mock_nlp.assert_called_once()
        mock_llm_cls.return_value.extract_missing.assert_called_once()

    @patch("wiki_pipeline.pipeline.download_dump")
    @patch("wiki_pipeline.pipeline.parse_page_dump")
    @patch("wiki_pipeline.pipeline.build_linktarget_map")
    @patch("wiki_pipeline.pipeline.parse_category_links")
    @patch("wiki_pipeline.pipeline.filter_pages_from_meta")
    @patch("wiki_pipeline.pipeline.WikiApiClient")
    @patch("wiki_pipeline.pipeline.LlmExtractor")
    @patch("wiki_pipeline.pipeline.extract_from_text")
    @patch("wiki_pipeline.pipeline.load_dotenv")
    def test_infobox_fills_all_skips_nlp_and_llm(
        self, mock_dotenv, mock_nlp, mock_llm_cls, mock_api_cls,
        mock_filter, mock_catlinks, mock_lt, mock_page, mock_download, tmp_path
    ):
        """When infobox fills all fields, neither NLP nor LLM is called."""
        mock_download.side_effect = lambda url, dest: dest
        mock_page.return_value = ({}, {1: ("A", 6000)})
        mock_lt.return_value = {}
        mock_catlinks.return_value = ParsedCategoryLinks(
            cat_pages={"Test_Category": {1}},
        )
        mock_filter.return_value = [PageInfo(1, "A", 6000)]
        api = mock_api_cls.return_value
        api.fetch_wikitext_batch.return_value = {
            "A": "{{Infobox person|birth_date={{birth date|1900|1|1}}|nationality=French|occupation=Painter|death_date={{death date|1970|1|1}}}}",
        }
        api.fetch_plaintext_batch.return_value = {"A": "text"}

        config = _config(tmp_path)
        run(config)

        mock_nlp.assert_not_called()
        mock_llm_cls.return_value.extract_missing.assert_not_called()
