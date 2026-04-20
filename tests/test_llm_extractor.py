"""Tests for llm_extractor — Ollama fallback (mocked requests)."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from wiki_pipeline.llm_extractor import LlmExtractor, _parse_json

REQUIRED = ("birth_date", "death_date", "nationality", "occupation")


def _mock_post(response_dict: dict):
    """Create a mock requests.post return value with given JSON body."""
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"response": json.dumps(response_dict)}
    return mock_resp


class TestLlmExtractor:
    @patch("wiki_pipeline.llm_extractor.requests.post")
    def test_skips_when_complete(self, mock_post):
        extractor = LlmExtractor()
        existing = {"birth_date": "1840-11-14", "death_date": "1926-12-05",
                     "nationality": "French", "occupation": "Painter"}
        result = extractor.extract_missing("text", existing, REQUIRED)
        assert result == existing
        mock_post.assert_not_called()

    @patch("wiki_pipeline.llm_extractor.requests.post")
    def test_fills_missing_fields(self, mock_post):
        mock_post.return_value = _mock_post({"nationality": "French", "occupation": "Painter"})
        extractor = LlmExtractor()
        existing = {"birth_date": "1840-11-14", "death_date": "1926-12-05",
                     "nationality": None, "occupation": None}
        result = extractor.extract_missing("Monet was French...", existing, REQUIRED)
        assert result["nationality"] == "French"
        assert result["occupation"] == "Painter"
        assert result["birth_date"] == "1840-11-14"  # preserved

    @patch("wiki_pipeline.llm_extractor.requests.post")
    def test_handles_malformed_json(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"response": "not json"}
        mock_post.return_value = mock_resp
        extractor = LlmExtractor()
        existing = {"birth_date": "1840", "nationality": None}
        result = extractor.extract_missing("text", existing, ("birth_date", "nationality"))
        assert result["nationality"] is None  # graceful fallback
        assert result["birth_date"] == "1840"

    @patch("wiki_pipeline.llm_extractor.requests.post")
    def test_null_llm_values_stay_none(self, mock_post):
        mock_post.return_value = _mock_post({"nationality": None})
        extractor = LlmExtractor()
        existing = {"nationality": None}
        result = extractor.extract_missing("text", existing, ("nationality",))
        assert result["nationality"] is None


class TestLlmExtractorEtymology:
    @patch("wiki_pipeline.llm_extractor.requests.post")
    def test_returns_etymology_key(self, mock_post):
        mock_post.return_value = _mock_post(
            {"etymology": "Named after Henry Hudson who explored it in 1609."}
        )
        extractor = LlmExtractor()
        result = extractor.extract_etymology("The Hudson River is a river in New York...")
        assert result["etymology"] == "Named after Henry Hudson who explored it in 1609."

    @patch("wiki_pipeline.llm_extractor.requests.post")
    def test_null_returns_none(self, mock_post):
        mock_post.return_value = _mock_post({"etymology": None})
        extractor = LlmExtractor()
        result = extractor.extract_etymology("Some river text.")
        assert result == {"etymology": None}

    @patch("wiki_pipeline.llm_extractor.requests.post")
    def test_malformed_json_returns_none(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"response": "not json"}
        mock_post.return_value = mock_resp
        extractor = LlmExtractor()
        result = extractor.extract_etymology("Some river text.")
        assert result == {"etymology": None}

    @patch("wiki_pipeline.llm_extractor.requests.post")
    def test_non_english_lang_hint(self, mock_post):
        mock_post.return_value = _mock_post({"etymology": "Du Latin flumen."})
        extractor = LlmExtractor()
        result = extractor.extract_etymology("Un fleuve en France.", lang="fr")
        assert result["etymology"] is not None
        call_kwargs = mock_post.call_args[1]
        prompt = call_kwargs["json"]["prompt"]
        assert "non-English" in prompt


class TestParseJson:
    def test_plain_json(self):
        assert _parse_json('{"a": 1}') == {"a": 1}

    def test_code_block(self):
        assert _parse_json('```json\n{"a": 1}\n```') == {"a": 1}

    def test_bare_code_block(self):
        assert _parse_json('```\n{"x": 2}\n```') == {"x": 2}

    def test_trailing_comma(self):
        assert _parse_json('{"a": 1,}') == {"a": 1}
