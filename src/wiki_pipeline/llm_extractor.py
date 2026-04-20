"""Ollama fallback extractor for missing infobox fields."""

from __future__ import annotations

import json
import logging
import os
import re
import time

import requests

logger = logging.getLogger(__name__)


class LlmExtractor:
    def __init__(
        self,
        model: str | None = None,
        base_url: str | None = None,
        timeout: int = 120,
        max_retries: int = 3,
    ):
        self.model = model or os.getenv("OLLAMA_MODEL", "llama3.1:8b")
        self.base_url = (base_url or os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")).rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries

    def _generate_json(self, prompt: str) -> dict:
        url = f"{self.base_url}/api/generate"
        payload = {"model": self.model, "prompt": prompt, "stream": False, "format": "json"}
        for attempt in range(self.max_retries):
            try:
                resp = requests.post(url, json=payload, timeout=self.timeout)
                resp.raise_for_status()
                return _parse_json(resp.json()["response"])
            except (requests.RequestException, KeyError) as e:
                if attempt == self.max_retries - 1:
                    raise
                wait = 2 ** attempt
                logger.warning("Ollama attempt %d failed, retrying in %ds: %s", attempt + 1, wait, e)
                time.sleep(wait)
        return {}

    def extract_missing(
        self,
        plain_text: str,
        existing: dict[str, str | None],
        required_fields: tuple[str, ...] | list[str],
        lang: str = "en",
    ) -> dict[str, str | None]:
        """Fill in None-valued fields using Ollama on truncated plain text."""
        missing = [f for f in required_fields if existing.get(f) is None]
        if not missing:
            return dict(existing)

        truncated = plain_text[:3000]
        lang_hint = " The text may be in a non-English language." if lang != "en" else ""
        prompt = (
            f"Extract these fields from the biography text below.{lang_hint} "
            f"Return ONLY a JSON object with these keys: {', '.join(missing)}. "
            f"Use null for any field you cannot determine.\n\nText:\n{truncated}"
        )

        try:
            data = self._generate_json(prompt)
        except Exception as e:
            logger.warning("LLM extraction failed: %s", e)
            return dict(existing)

        result = dict(existing)
        for f in missing:
            val = data.get(f)
            if val is not None and isinstance(val, str) and val.strip():
                result[f] = val.strip()
        return result

    def extract_etymology(
        self,
        plain_text: str,
        lang: str = "en",
    ) -> dict[str, str | None]:
        """Extract etymology/name-origin from plain text using Ollama."""
        truncated = plain_text[:3000]
        lang_hint = " The text may be in a non-English language." if lang != "en" else ""
        prompt = (
            f"Extract the etymology or origin of the name from the geographic feature article below.{lang_hint} "
            f"Return ONLY a JSON object with a single key 'etymology' whose value is a concise "
            f"1-3 sentence explanation of how the place got its name, or null if not determinable.\n\nText:\n{truncated}"
        )
        try:
            data = self._generate_json(prompt)
        except Exception as e:
            logger.warning("LLM etymology extraction failed: %s", e)
            return {"etymology": None}
        val = data.get("etymology")
        if val and isinstance(val, str) and val.strip():
            return {"etymology": val.strip()}
        return {"etymology": None}


def _parse_json(text: str) -> dict:
    text = text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    cleaned = re.sub(r",\s*([}\]])", r"\1", text)
    return json.loads(cleaned)
