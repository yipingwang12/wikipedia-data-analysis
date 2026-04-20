"""Tests for llm_extractor — Claude Haiku fallback (mocked anthropic client)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from wiki_pipeline.llm_extractor import LlmExtractor, _extract_json

REQUIRED = ("birth_date", "death_date", "nationality", "occupation")


def _mock_response(text: str):
    """Create a mock anthropic response."""
    content_block = SimpleNamespace(text=text)
    return SimpleNamespace(content=[content_block])


class TestLlmExtractor:
    @patch("wiki_pipeline.llm_extractor.anthropic.Anthropic")
    def test_skips_when_complete(self, mock_cls):
        extractor = LlmExtractor()
        existing = {"birth_date": "1840-11-14", "death_date": "1926-12-05",
                     "nationality": "French", "occupation": "Painter"}
        result = extractor.extract_missing("text", existing, REQUIRED)
        assert result == existing
        extractor.client.messages.create.assert_not_called()

    @patch("wiki_pipeline.llm_extractor.anthropic.Anthropic")
    def test_fills_missing_fields(self, mock_cls):
        extractor = LlmExtractor()
        extractor.client.messages.create.return_value = _mock_response(
            '{"nationality": "French", "occupation": "Painter"}'
        )
        existing = {"birth_date": "1840-11-14", "death_date": "1926-12-05",
                     "nationality": None, "occupation": None}
        result = extractor.extract_missing("Monet was French...", existing, REQUIRED)
        assert result["nationality"] == "French"
        assert result["occupation"] == "Painter"
        assert result["birth_date"] == "1840-11-14"  # preserved

    @patch("wiki_pipeline.llm_extractor.anthropic.Anthropic")
    def test_handles_malformed_json(self, mock_cls):
        extractor = LlmExtractor()
        extractor.client.messages.create.return_value = _mock_response("not json")
        existing = {"birth_date": "1840", "nationality": None}
        result = extractor.extract_missing("text", existing, ("birth_date", "nationality"))
        assert result["nationality"] is None  # graceful fallback
        assert result["birth_date"] == "1840"

    @patch("wiki_pipeline.llm_extractor.anthropic.Anthropic")
    def test_handles_code_block_wrapper(self, mock_cls):
        extractor = LlmExtractor()
        extractor.client.messages.create.return_value = _mock_response(
            '```json\n{"nationality": "Dutch"}\n```'
        )
        existing = {"nationality": None}
        result = extractor.extract_missing("text", existing, ("nationality",))
        assert result["nationality"] == "Dutch"

    @patch("wiki_pipeline.llm_extractor.anthropic.Anthropic")
    def test_null_llm_values_stay_none(self, mock_cls):
        extractor = LlmExtractor()
        extractor.client.messages.create.return_value = _mock_response(
            '{"nationality": null}'
        )
        existing = {"nationality": None}
        result = extractor.extract_missing("text", existing, ("nationality",))
        assert result["nationality"] is None


class TestLlmExtractorEtymology:
    @patch("wiki_pipeline.llm_extractor.anthropic.Anthropic")
    def test_returns_etymology_key(self, mock_cls):
        extractor = LlmExtractor()
        extractor.client.messages.create.return_value = _mock_response(
            '{"etymology": "Named after Henry Hudson who explored it in 1609."}'
        )
        result = extractor.extract_etymology("The Hudson River is a river in New York...")
        assert result["etymology"] == "Named after Henry Hudson who explored it in 1609."

    @patch("wiki_pipeline.llm_extractor.anthropic.Anthropic")
    def test_null_returns_none(self, mock_cls):
        extractor = LlmExtractor()
        extractor.client.messages.create.return_value = _mock_response('{"etymology": null}')
        result = extractor.extract_etymology("Some river text.")
        assert result == {"etymology": None}

    @patch("wiki_pipeline.llm_extractor.anthropic.Anthropic")
    def test_malformed_json_returns_none(self, mock_cls):
        extractor = LlmExtractor()
        extractor.client.messages.create.return_value = _mock_response("not json")
        result = extractor.extract_etymology("Some river text.")
        assert result == {"etymology": None}

    @patch("wiki_pipeline.llm_extractor.anthropic.Anthropic")
    def test_non_english_lang_hint(self, mock_cls):
        extractor = LlmExtractor()
        extractor.client.messages.create.return_value = _mock_response('{"etymology": "Du Latin flumen."}')
        result = extractor.extract_etymology("Un fleuve en France.", lang="fr")
        assert result["etymology"] is not None
        call_args = extractor.client.messages.create.call_args
        prompt = call_args[1]["messages"][0]["content"]
        assert "non-English" in prompt


class TestExtractJson:
    def test_plain_json(self):
        assert _extract_json('{"a": 1}') == '{"a": 1}'

    def test_code_block(self):
        text = "```json\n{\"a\": 1}\n```"
        assert _extract_json(text).strip() == '{"a": 1}'

    def test_bare_code_block(self):
        text = "```\n{\"x\": 2}\n```"
        assert _extract_json(text).strip() == '{"x": 2}'
