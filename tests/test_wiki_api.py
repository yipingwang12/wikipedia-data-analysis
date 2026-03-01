"""Tests for wiki_api — MediaWiki API client (mocked with responses)."""

from __future__ import annotations

import json

import responses

from wiki_pipeline.wiki_api import WikiApiClient

API_URL = "https://en.wikipedia.org/w/api.php"


class TestWikiApiClient:
    def _client(self, **kwargs) -> WikiApiClient:
        return WikiApiClient(api_url=API_URL, rate_limit_s=0, **kwargs)

    @responses.activate
    def test_fetch_wikitext_batch(self):
        responses.add(
            responses.GET,
            API_URL,
            json={
                "query": {
                    "pages": [
                        {
                            "title": "Claude Monet",
                            "revisions": [
                                {"slots": {"main": {"content": "{{Infobox person}}"}}}
                            ],
                        }
                    ]
                }
            },
        )
        client = self._client()
        result = client.fetch_wikitext_batch(["Claude Monet"])
        assert "Claude Monet" in result
        assert "Infobox person" in result["Claude Monet"]

    @responses.activate
    def test_fetch_plaintext_batch(self):
        responses.add(
            responses.GET,
            API_URL,
            json={
                "query": {
                    "pages": [
                        {"title": "Claude Monet", "extract": "Claude Monet was a painter."}
                    ]
                }
            },
        )
        client = self._client()
        result = client.fetch_plaintext_batch(["Claude Monet"])
        assert result["Claude Monet"] == "Claude Monet was a painter."

    @responses.activate
    def test_batch_splitting(self):
        """Titles exceeding batch_size trigger multiple requests."""
        call_count = 0

        def callback(request):
            nonlocal call_count
            call_count += 1
            titles = request.params.get("titles", "").split("|")
            pages = [{"title": t, "extract": f"text for {t}"} for t in titles]
            return (200, {}, json.dumps({"query": {"pages": pages}}))

        responses.add_callback(responses.GET, API_URL, callback=callback)
        client = self._client(batch_size=2)
        result = client.fetch_plaintext_batch(["A", "B", "C"])
        assert call_count == 2
        assert len(result) == 3

    @responses.activate
    def test_continuation(self):
        """Handles continuation tokens across multiple requests."""
        responses.add(
            responses.GET,
            API_URL,
            json={
                "query": {"pages": [{"title": "A", "extract": "text A"}]},
                "continue": {"excontinue": "B", "continue": "||"},
            },
        )
        responses.add(
            responses.GET,
            API_URL,
            json={"query": {"pages": [{"title": "B", "extract": "text B"}]}},
        )
        client = self._client()
        result = client.fetch_plaintext_batch(["A", "B"])
        assert "A" in result
        assert "B" in result

    @responses.activate
    def test_missing_page_skipped(self):
        responses.add(
            responses.GET,
            API_URL,
            json={
                "query": {
                    "pages": [
                        {"title": "Missing", "missing": True}
                    ]
                }
            },
        )
        client = self._client()
        result = client.fetch_plaintext_batch(["Missing"])
        assert "Missing" not in result
