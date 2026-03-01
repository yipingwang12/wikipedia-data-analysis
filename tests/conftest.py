"""Shared fixtures for wiki_pipeline tests."""

from __future__ import annotations

import gzip
import io
from pathlib import Path

import pytest


FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def fixtures_dir():
    return FIXTURES_DIR


def make_sql_gz(content: str) -> Path:
    """Create a temporary gzipped SQL file and return its path."""
    import tempfile
    tmp = tempfile.NamedTemporaryFile(suffix=".sql.gz", delete=False)
    with gzip.open(tmp.name, "wt") as f:
        f.write(content)
    return Path(tmp.name)


def make_sql_lines(content: str) -> io.StringIO:
    """Create a StringIO from SQL content for in-memory testing."""
    return io.StringIO(content)
