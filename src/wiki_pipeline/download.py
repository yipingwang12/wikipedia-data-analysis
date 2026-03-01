"""Dump downloader with resume support, progress bar, and exponential backoff."""

from __future__ import annotations

import logging
import sys
import time
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

USER_AGENT = "WikiPipeline/0.1 (educational data analysis; contact: wiki-pipeline@example.com)"
BACKOFF_BASE_S = 60
BACKOFF_FACTOR = 2
MAX_RETRIES = 5


def download_dump(url: str, dest: Path) -> Path:
    """Download a file from url to dest with resume and exponential backoff.

    Skips download if dest exists and matches Content-Length.
    Downloads to .partial file, then atomically renames.
    Resumes from existing .partial file if present.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    headers = {"User-Agent": USER_AGENT}

    # Check remote size
    head = requests.head(url, headers=headers, timeout=30)
    head.raise_for_status()
    remote_size = int(head.headers.get("Content-Length", 0))

    # Skip if already downloaded
    if dest.exists() and dest.stat().st_size == remote_size and remote_size > 0:
        print(f"  Already downloaded: {dest.name}")
        return dest

    partial = dest.with_suffix(dest.suffix + ".partial")
    delay = BACKOFF_BASE_S

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            _download_stream(url, partial, headers, remote_size)
            partial.rename(dest)
            return dest
        except (requests.RequestException, IOError, ConnectionError) as e:
            logger.warning("Download attempt %d/%d failed: %s", attempt, MAX_RETRIES, e)
            if attempt == MAX_RETRIES:
                raise
            time.sleep(delay)
            delay *= BACKOFF_FACTOR

    raise RuntimeError("Unreachable")


def _download_stream(url: str, dest: Path, headers: dict[str, str], total: int) -> None:
    """Stream download to dest with resume support and progress bar."""
    resume_pos = 0
    mode = "wb"
    req_headers = dict(headers)

    if dest.exists():
        resume_pos = dest.stat().st_size
        if total and resume_pos < total:
            req_headers["Range"] = f"bytes={resume_pos}-"
            mode = "ab"
        elif resume_pos >= total > 0:
            return  # already complete

    with requests.get(url, headers=req_headers, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(dest, mode) as f:
            downloaded = resume_pos
            start_time = time.time()
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
                downloaded += len(chunk)
                _print_progress(dest.name, downloaded, total, start_time, resume_pos)
    print()  # newline after progress bar


def _print_progress(
    filename: str, downloaded: int, total: int, start_time: float, resume_pos: int
) -> None:
    """Print a progress bar to stderr."""
    if not total:
        return
    pct = downloaded / total * 100
    bar_width = 30
    filled = int(bar_width * downloaded / total)
    bar = "█" * filled + "░" * (bar_width - filled)

    elapsed = time.time() - start_time
    new_bytes = downloaded - resume_pos
    speed = new_bytes / elapsed if elapsed > 0 else 0
    remaining = (total - downloaded) / speed if speed > 0 else 0

    dl_mb = downloaded / 1024 / 1024
    total_mb = total / 1024 / 1024
    speed_mb = speed / 1024 / 1024

    sys.stderr.write(
        f"\r  {filename}: {bar} {pct:5.1f}% "
        f"({dl_mb:.0f}/{total_mb:.0f} MB) "
        f"{speed_mb:.1f} MB/s  ETA {int(remaining // 60)}m{int(remaining % 60):02d}s"
    )
    sys.stderr.flush()
