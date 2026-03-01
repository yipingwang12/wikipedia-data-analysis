"""Tests for cache — pickle save/load, mtime validation, clear."""

from __future__ import annotations

import pickle
import time
from pathlib import Path

from wiki_pipeline.cache import (
    MTIME_EPSILON,
    CacheMetadata,
    cache_dir,
    clear_cache,
    load_pickle,
    save_pickle,
)


def _make_source(tmp_path: Path, name: str, content: str = "data") -> Path:
    p = tmp_path / name
    p.write_text(content)
    return p


class TestCacheDir:
    def test_returns_dot_cache(self, tmp_path):
        assert cache_dir(tmp_path) == tmp_path / ".cache"


class TestSaveAndLoadPickle:
    def test_round_trip(self, tmp_path):
        src = _make_source(tmp_path, "dump.sql.gz")
        cp = tmp_path / ".cache" / "test.pkl"
        data = {"key": [1, 2, 3]}
        save_pickle(data, cp, [src])
        loaded = load_pickle(cp, [src])
        assert loaded == data

    def test_creates_parent_dirs(self, tmp_path):
        src = _make_source(tmp_path, "dump.sql.gz")
        cp = tmp_path / "deep" / "nested" / "test.pkl"
        save_pickle("hello", cp, [src])
        assert cp.exists()

    def test_meta_sidecar_exists(self, tmp_path):
        src = _make_source(tmp_path, "dump.sql.gz")
        cp = tmp_path / ".cache" / "test.pkl"
        save_pickle(42, cp, [src])
        assert cp.with_suffix(".pkl.meta").exists()

    def test_returns_none_on_missing(self, tmp_path):
        src = _make_source(tmp_path, "dump.sql.gz")
        assert load_pickle(tmp_path / "nonexistent.pkl", [src]) is None

    def test_returns_none_on_missing_meta(self, tmp_path):
        src = _make_source(tmp_path, "dump.sql.gz")
        cp = tmp_path / "test.pkl"
        cp.write_bytes(pickle.dumps("data"))
        assert load_pickle(cp, [src]) is None

    def test_stale_mtime_returns_none(self, tmp_path):
        src = _make_source(tmp_path, "dump.sql.gz")
        cp = tmp_path / ".cache" / "test.pkl"
        save_pickle("old", cp, [src])
        # Modify source to change mtime
        time.sleep(0.05)
        src.write_text("modified")
        assert load_pickle(cp, [src]) is None

    def test_missing_source_in_meta_returns_none(self, tmp_path):
        src1 = _make_source(tmp_path, "a.gz")
        src2 = _make_source(tmp_path, "b.gz")
        cp = tmp_path / ".cache" / "test.pkl"
        save_pickle("data", cp, [src1])
        assert load_pickle(cp, [src1, src2]) is None

    def test_multiple_sources(self, tmp_path):
        s1 = _make_source(tmp_path, "a.gz")
        s2 = _make_source(tmp_path, "b.gz")
        cp = tmp_path / ".cache" / "test.pkl"
        save_pickle({"multi": True}, cp, [s1, s2])
        assert load_pickle(cp, [s1, s2]) == {"multi": True}

    def test_corrupted_meta_returns_none(self, tmp_path):
        src = _make_source(tmp_path, "dump.sql.gz")
        cp = tmp_path / ".cache" / "test.pkl"
        save_pickle("data", cp, [src])
        cp.with_suffix(".pkl.meta").write_text("not a pickle")
        assert load_pickle(cp, [src]) is None

    def test_corrupted_data_returns_none(self, tmp_path):
        src = _make_source(tmp_path, "dump.sql.gz")
        cp = tmp_path / ".cache" / "test.pkl"
        save_pickle("data", cp, [src])
        cp.write_text("not a pickle")
        assert load_pickle(cp, [src]) is None


class TestClearCache:
    def test_empty_dir(self, tmp_path):
        assert clear_cache(tmp_path) == 0

    def test_removes_files(self, tmp_path):
        d = tmp_path / ".cache"
        d.mkdir()
        (d / "a.pkl").write_text("x")
        (d / "b.pkl.meta").write_text("y")
        assert clear_cache(tmp_path) == 2
        assert list(d.iterdir()) == []

    def test_nonexistent_cache_dir(self, tmp_path):
        assert clear_cache(tmp_path / "nope") == 0


class TestCacheMetadata:
    def test_frozen(self):
        m = CacheMetadata(source_mtimes={"a": 1.0}, created_at=1.0)
        assert m.source_mtimes == {"a": 1.0}
