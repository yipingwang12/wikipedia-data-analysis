"""Claude Haiku fallback extractor for missing infobox fields."""

from __future__ import annotations

import json
import logging

import anthropic

logger = logging.getLogger(__name__)


class LlmExtractor:
    def __init__(self, model: str = "claude-haiku-4-5-20251001"):
        self._client = None
        self.model = model

    @property
    def client(self) -> anthropic.Anthropic:
        if self._client is None:
            self._client = anthropic.Anthropic()
        return self._client

    def extract_missing(
        self,
        plain_text: str,
        existing: dict[str, str | None],
        required_fields: tuple[str, ...] | list[str],
        lang: str = "en",
    ) -> dict[str, str | None]:
        """Fill in None-valued fields using Claude Haiku on truncated plain text.

        Returns merged dict with existing values preserved and gaps filled where possible.
        """
        missing = [f for f in required_fields if existing.get(f) is None]
        if not missing:
            return dict(existing)

        truncated = plain_text[:3000]
        lang_hint = " The text may be in a non-English language." if lang != "en" else ""
        prompt = (
            f"Extract these fields from the biography text below.{lang_hint} "
            f"Return ONLY a JSON object with these keys: {', '.join(missing)}. "
            f"Use null for any field you cannot determine.\n\n"
            f"Text:\n{truncated}"
        )

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=256,
                messages=[{"role": "user", "content": prompt}],
            )
            text = response.content[0].text.strip()
            # Extract JSON from response (may be wrapped in markdown code block)
            text = _extract_json(text)
            data = json.loads(text)
        except (json.JSONDecodeError, anthropic.APIError, ImportError, IndexError, KeyError) as e:
            logger.warning("LLM extraction failed: %s", e)
            return dict(existing)

        result = dict(existing)
        for f in missing:
            val = data.get(f)
            if val is not None and isinstance(val, str) and val.strip():
                result[f] = val.strip()
        return result


def _extract_json(text: str) -> str:
    """Strip markdown code fences if present."""
    if "```" in text:
        lines = text.split("\n")
        inside = False
        json_lines = []
        for line in lines:
            if line.strip().startswith("```"):
                inside = not inside
                continue
            if inside:
                json_lines.append(line)
        return "\n".join(json_lines)
    return text
