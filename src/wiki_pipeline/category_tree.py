"""Build category ID map, adjacency list, and BFS collection of article IDs."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field

from .sql_parser import iter_rows


@dataclass
class CategoryTree:
    subcategories: dict[str, set[str]] = field(default_factory=dict)
    article_ids: set[int] = field(default_factory=set)


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


def bfs_from_root(parsed: ParsedCategoryLinks, root_category: str) -> CategoryTree:
    """BFS over pre-parsed adjacency — milliseconds."""
    tree = CategoryTree()
    visited: set[str] = set()
    queue: deque[str] = deque([root_category])

    while queue:
        cat = queue.popleft()
        if cat in visited:
            continue
        visited.add(cat)

        if cat in parsed.children:
            tree.subcategories[cat] = parsed.children[cat]
            for child in parsed.children[cat]:
                if child not in visited:
                    queue.append(child)

        if cat in parsed.cat_pages:
            tree.article_ids.update(parsed.cat_pages[cat])

    return tree


def build_category_tree(
    catlinks_path: str | object,
    root_category: str,
    catid_to_name: dict[int, str],
    lt_to_name: dict[int, str] | None = None,
) -> CategoryTree:
    """Stream categorylinks.sql.gz, BFS from root to collect subcategories and article IDs.

    Thin wrapper: parse_category_links → bfs_from_root.
    """
    parsed = parse_category_links(catlinks_path, catid_to_name, lt_to_name)
    return bfs_from_root(parsed, root_category)
