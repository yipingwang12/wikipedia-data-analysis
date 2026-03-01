"""MediaWiki API client for fetching wikitext and plaintext extracts."""

from __future__ import annotations

import time

import requests


class WikiApiClient:
    def __init__(
        self,
        api_url: str = "https://en.wikipedia.org/w/api.php",
        batch_size: int = 50,
        rate_limit_s: float = 0.1,
    ):
        self.api_url = api_url
        self.batch_size = batch_size
        self.rate_limit_s = rate_limit_s
        self.session = requests.Session()
        self.session.headers["User-Agent"] = (
            "WikiPipeline/0.1 (educational data analysis; contact: wiki-pipeline@example.com)"
        )

    def fetch_wikitext_batch(self, titles: list[str]) -> dict[str, str]:
        """Fetch raw wikitext for up to batch_size titles."""
        result: dict[str, str] = {}
        for i in range(0, len(titles), self.batch_size):
            batch = titles[i : i + self.batch_size]
            params = {
                "action": "query",
                "titles": "|".join(batch),
                "prop": "revisions",
                "rvprop": "content",
                "rvslots": "main",
                "format": "json",
                "formatversion": "2",
            }
            data = self._query(params)
            for page in data.get("query", {}).get("pages", []):
                title = page.get("title", "")
                revs = page.get("revisions", [])
                if revs:
                    content = revs[0].get("slots", {}).get("main", {}).get("content", "")
                    result[title] = content
            time.sleep(self.rate_limit_s)
        return result

    def fetch_plaintext_batch(self, titles: list[str]) -> dict[str, str]:
        """Fetch plain-text extracts for up to batch_size titles."""
        result: dict[str, str] = {}
        for i in range(0, len(titles), self.batch_size):
            batch = titles[i : i + self.batch_size]
            params = {
                "action": "query",
                "titles": "|".join(batch),
                "prop": "extracts",
                "explaintext": "1",
                "format": "json",
                "formatversion": "2",
            }
            data = self._query(params)
            for page in data.get("query", {}).get("pages", []):
                title = page.get("title", "")
                extract = page.get("extract", "")
                if extract:
                    result[title] = extract
            time.sleep(self.rate_limit_s)
        return result

    def _query(self, params: dict) -> dict:
        """Execute API query, handling continuation."""
        result: dict = {}
        while True:
            resp = self.session.get(self.api_url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if "query" in data:
                if "query" not in result:
                    result["query"] = data["query"]
                else:
                    result["query"]["pages"].extend(data["query"].get("pages", []))
            if "continue" not in data:
                break
            params.update(data["continue"])
            time.sleep(self.rate_limit_s)
        return result
