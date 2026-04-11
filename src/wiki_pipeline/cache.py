"""Pickle-based caching with mtime-validated sidecar metadata."""

from __future__ import annotations

import pickle
import time
from dataclasses import asdict, dataclass
from pathlib import Path

MTIME_EPSILON = 0.01


@dataclass(frozen=True)
class CacheMetadata:
    source_mtimes: dict[str, float]  # {dump_filename: mtime}
    created_at: float


def cache_dir(data_dir: Path, wiki: str = "enwiki") -> Path:
    return data_dir / ".cache" / wiki


def save_pickle(data: object, cache_path: Path, source_paths: list[Path]) -> None:
    """Serialize data to cache_path with a .meta sidecar tracking source mtimes."""
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    meta = CacheMetadata(
        source_mtimes={p.name: p.stat().st_mtime for p in source_paths},
        created_at=time.time(),
    )
    with open(cache_path, "wb") as f:
        pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
    meta_path = cache_path.with_suffix(cache_path.suffix + ".meta")
    with open(meta_path, "wb") as f:
        pickle.dump(asdict(meta), f, protocol=pickle.HIGHEST_PROTOCOL)


def load_pickle(cache_path: Path, source_paths: list[Path]) -> object | None:
    """Load cached data if cache_path exists and source mtimes match. None on miss/stale."""
    meta_path = cache_path.with_suffix(cache_path.suffix + ".meta")
    if not cache_path.exists() or not meta_path.exists():
        return None
    try:
        with open(meta_path, "rb") as f:
            raw = pickle.load(f)  # noqa: S301
        meta = CacheMetadata(**raw)
    except Exception:
        return None
    for p in source_paths:
        expected = meta.source_mtimes.get(p.name)
        if expected is None:
            return None
        if not p.exists():
            continue  # source deleted; trust existing cache
        if abs(p.stat().st_mtime - expected) > MTIME_EPSILON:
            return None
    try:
        with open(cache_path, "rb") as f:
            return pickle.load(f)  # noqa: S301
    except Exception:
        return None


def clear_cache(data_dir: Path, wiki: str = "enwiki") -> int:
    """Remove all files from cache dir for a specific wiki, return count removed."""
    d = cache_dir(data_dir, wiki)
    if not d.exists():
        return 0
    count = 0
    for f in d.iterdir():
        if f.is_file():
            f.unlink()
            count += 1
    return count
