"""Streaming MySQL INSERT parser — index-based scanning, O(1) memory per row."""

from __future__ import annotations

import gzip
from collections.abc import Iterator


def iter_rows(gz_path: str | object, table_name: str) -> Iterator[tuple[str | None, ...]]:
    """Yield tuples of column values from INSERT statements in a gzipped SQL dump.

    Handles quoting, backslash escapes, NULL literals, commas inside strings,
    and bare (unquoted) numeric values.
    gz_path can be a path string or a file-like object (for testing).
    """
    prefix = f"INSERT INTO `{table_name}` VALUES "

    if hasattr(gz_path, "read"):
        lines = gz_path
    else:
        lines = gzip.open(gz_path, "rt", errors="replace")

    try:
        for line in lines:
            if not line.startswith(prefix):
                continue
            yield from _parse_inserts(line[len(prefix):])
    finally:
        if not hasattr(gz_path, "read"):
            lines.close()


def _parse_inserts(data: str) -> Iterator[tuple[str | None, ...]]:
    """Parse the VALUES portion of an INSERT statement.

    Uses str.find() for C-speed scanning instead of character-by-character
    iteration, critical for multi-hundred-MB INSERT lines in Wikipedia dumps.
    """
    n = len(data)
    i = 0

    while i < n:
        # Find start of next row
        i = data.find("(", i)
        if i == -1:
            break
        i += 1

        fields: list[str | None] = []

        while i < n:
            ch = data[i]

            if ch == "'":
                # Quoted string — scan for closing quote using str.find()
                i += 1
                parts: list[str] = []
                while True:
                    q = data.find("'", i)
                    b = data.find("\\", i)
                    if b != -1 and b < q:
                        parts.append(data[i:b])
                        if b + 1 < n:
                            parts.append(data[b + 1])
                        i = b + 2
                    else:
                        parts.append(data[i:q])
                        i = q + 1
                        break
                fields.append("".join(parts))

            elif ch == ")":
                yield tuple(fields)
                i += 1
                break

            elif ch == ",":
                i += 1

            elif ch == " " or ch == "\t":
                i += 1

            else:
                # Bare value (number, NULL, etc.)
                j = i + 1
                while j < n:
                    c = data[j]
                    if c == "," or c == ")":
                        break
                    j += 1
                val = data[i:j]
                fields.append(None if val == "NULL" else val)
                i = j
