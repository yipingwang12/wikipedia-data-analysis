"""Tests for download — dump downloader with resume + backoff (mocked HTTP)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
import responses

from wiki_pipeline.download import download_dump


class TestDownloadDump:
    @responses.activate
    def test_downloads_file(self, tmp_path: Path):
        url = "https://dumps.wikimedia.org/test.sql.gz"
        dest = tmp_path / "test.sql.gz"
        responses.add(responses.HEAD, url, headers={"Content-Length": "5"})
        responses.add(responses.GET, url, body=b"hello", stream=True)
        result = download_dump(url, dest)
        assert result == dest
        assert dest.read_bytes() == b"hello"

    @responses.activate
    def test_skips_existing_file(self, tmp_path: Path):
        url = "https://dumps.wikimedia.org/test.sql.gz"
        dest = tmp_path / "test.sql.gz"
        dest.write_bytes(b"hello")
        responses.add(responses.HEAD, url, headers={"Content-Length": "5"})
        result = download_dump(url, dest)
        assert result == dest
        assert len(responses.calls) == 1  # only HEAD, no GET

    @responses.activate
    def test_creates_parent_dirs(self, tmp_path: Path):
        url = "https://dumps.wikimedia.org/test.sql.gz"
        dest = tmp_path / "sub" / "dir" / "test.sql.gz"
        responses.add(responses.HEAD, url, headers={"Content-Length": "3"})
        responses.add(responses.GET, url, body=b"abc", stream=True)
        result = download_dump(url, dest)
        assert result.exists()

    @responses.activate
    @patch("wiki_pipeline.download.time.sleep")
    def test_retries_on_failure(self, mock_sleep, tmp_path: Path):
        url = "https://dumps.wikimedia.org/test.sql.gz"
        dest = tmp_path / "test.sql.gz"
        responses.add(responses.HEAD, url, headers={"Content-Length": "5"})
        responses.add(responses.GET, url, body=ConnectionError("network error"))
        responses.add(responses.GET, url, body=b"hello", stream=True)
        result = download_dump(url, dest)
        assert result == dest
        mock_sleep.assert_called_once_with(60)

    @responses.activate
    @patch("wiki_pipeline.download.time.sleep")
    def test_exponential_backoff(self, mock_sleep, tmp_path: Path):
        url = "https://dumps.wikimedia.org/test.sql.gz"
        dest = tmp_path / "test.sql.gz"
        responses.add(responses.HEAD, url, headers={"Content-Length": "5"})
        responses.add(responses.GET, url, body=ConnectionError("err"))
        responses.add(responses.GET, url, body=ConnectionError("err"))
        responses.add(responses.GET, url, body=b"hello", stream=True)
        download_dump(url, dest)
        calls = [c.args[0] for c in mock_sleep.call_args_list]
        assert calls == [60, 120]

    @responses.activate
    def test_removes_partial_on_success(self, tmp_path: Path):
        url = "https://dumps.wikimedia.org/test.sql.gz"
        dest = tmp_path / "test.sql.gz"
        responses.add(responses.HEAD, url, headers={"Content-Length": "5"})
        responses.add(responses.GET, url, body=b"hello", stream=True)
        download_dump(url, dest)
        assert not dest.with_suffix(".gz.partial").exists()
