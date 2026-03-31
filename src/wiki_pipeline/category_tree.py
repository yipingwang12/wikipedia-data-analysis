"""Build category ID map, adjacency list, BFS/regex collection of article IDs."""

from __future__ import annotations

import re
from collections import deque
from dataclasses import dataclass, field
from typing import Iterable

from .sql_parser import iter_rows


@dataclass
class CategoryTree:
    subcategories: dict[str, set[str]] = field(default_factory=dict)
    article_ids: set[int] = field(default_factory=set)
    depth_stats: list[tuple[int, int]] = field(default_factory=list)  # [(cats, articles), ...] per depth


@dataclass
class ParsedCategoryLinks:
    children: dict[str, set[str]] = field(default_factory=dict)   # parent → {child_names}
    cat_pages: dict[str, set[int]] = field(default_factory=dict)  # cat_name → {page_ids}


def build_catid_map(page_path: str | object) -> dict[int, str]:
    """Stream page.sql.gz, return {page_id: title} for namespace 14 (Category)."""
    catid_map: dict[int, str] = {}
    for row in iter_rows(page_path, "page"):
        if row[1] == "14":
            catid_map[int(row[0])] = row[2]
    return catid_map


def build_linktarget_map(lt_path: str | object) -> dict[int, str]:
    """Stream linktarget.sql.gz, return {lt_id: title} for namespace 14 (Category).

    Used to resolve cl_target_id in categorylinks (2025+ schema).
    """
    lt_map: dict[int, str] = {}
    for row in iter_rows(lt_path, "linktarget"):
        if row[1] == "14":
            lt_map[int(row[0])] = row[2]
    return lt_map


def parse_page_dump(
    page_path: str | object,
) -> tuple[dict[int, str], dict[int, tuple[str, int]]]:
    """Single-pass page.sql.gz → (catid_map, page_meta).

    catid_map: {page_id: title} for ns=14 (categories)
    page_meta: {page_id: (title, length)} for ns=0, non-redirect
    """
    catid_map: dict[int, str] = {}
    page_meta: dict[int, tuple[str, int]] = {}
    for row in iter_rows(page_path, "page"):
        page_id = int(row[0])
        ns = row[1]
        title = row[2]
        if ns == "14":
            catid_map[page_id] = title
        elif ns == "0" and row[3] != "1":
            page_meta[page_id] = (title, int(row[9]))
    return catid_map, page_meta


def parse_category_links(
    catlinks_path: str | object,
    catid_to_name: dict[int, str],
    lt_to_name: dict[int, str] | None = None,
) -> ParsedCategoryLinks:
    """Stream categorylinks.sql.gz, build FULL adjacency (no BFS, no root dependency)."""
    if lt_to_name is None:
        lt_to_name = {}

    children: dict[str, set[str]] = {}
    cat_pages: dict[str, set[int]] = {}

    for row in iter_rows(catlinks_path, "categorylinks"):
        cl_from = int(row[0])
        cl_type = row[4]
        cl_target_id = int(row[6])

        parent_name = lt_to_name.get(cl_target_id)
        if parent_name is None:
            continue

        if cl_type == "subcat":
            child_name = catid_to_name.get(cl_from)
            if child_name:
                children.setdefault(parent_name, set()).add(child_name)
        elif cl_type == "page":
            cat_pages.setdefault(parent_name, set()).add(cl_from)

    return ParsedCategoryLinks(children=children, cat_pages=cat_pages)


def bfs_from_root(
    parsed: ParsedCategoryLinks,
    root_category: str,
    max_depth: int | None = None,
) -> CategoryTree:
    """BFS over pre-parsed adjacency with optional depth limit.

    max_depth controls how many levels below root to expand.
    depth=0 is root itself. None means unlimited.
    Articles at the deepest allowed level are still collected.
    """
    tree = CategoryTree()
    visited: set[str] = set()
    queue: deque[tuple[str, int]] = deque([(root_category, 0)])

    # Per-depth accumulators: depth -> (cats_count, new_article_count)
    depth_cats: dict[int, int] = {}
    depth_articles: dict[int, int] = {}

    while queue:
        cat, depth = queue.popleft()
        if cat in visited:
            continue
        visited.add(cat)

        depth_cats[depth] = depth_cats.get(depth, 0) + 1

        # Expand children only if within depth limit
        if cat in parsed.children and (max_depth is None or depth < max_depth):
            tree.subcategories[cat] = parsed.children[cat]
            for child in parsed.children[cat]:
                if child not in visited:
                    queue.append((child, depth + 1))

        if cat in parsed.cat_pages:
            new_ids = parsed.cat_pages[cat] - tree.article_ids
            depth_articles[depth] = depth_articles.get(depth, 0) + len(new_ids)
            tree.article_ids.update(new_ids)

    max_d = max(depth_cats) if depth_cats else -1
    tree.depth_stats = [
        (depth_cats.get(d, 0), depth_articles.get(d, 0))
        for d in range(max_d + 1)
    ]

    return tree


def build_category_tree(
    catlinks_path: str | object,
    root_category: str,
    catid_to_name: dict[int, str],
    lt_to_name: dict[int, str] | None = None,
    max_depth: int | None = None,
) -> CategoryTree:
    """Stream categorylinks.sql.gz, BFS from root to collect subcategories and article IDs.

    Thin wrapper: parse_category_links → bfs_from_root.
    """
    parsed = parse_category_links(catlinks_path, catid_to_name, lt_to_name)
    return bfs_from_root(parsed, root_category, max_depth=max_depth)


def find_categories_by_regex(
    all_category_names: Iterable[str],
    patterns: Iterable[str],
) -> set[str]:
    """Return category names matching any of the given regex patterns (case-insensitive)."""
    compiled = [re.compile(p, re.IGNORECASE) for p in patterns]
    matched: set[str] = set()
    for name in all_category_names:
        for regex in compiled:
            if regex.search(name):
                matched.add(name)
                break
    return matched


def collect_articles_from_categories(
    parsed: ParsedCategoryLinks,
    category_names: set[str],
) -> set[int]:
    """Collect all article page IDs from the given set of categories."""
    article_ids: set[int] = set()
    for cat in category_names:
        ids = parsed.cat_pages.get(cat)
        if ids:
            article_ids.update(ids)
    return article_ids
