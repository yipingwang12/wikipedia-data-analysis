"""Tests for category_tree — catid map building, linktarget, BFS traversal, and new split functions."""

from __future__ import annotations

import io

from wiki_pipeline.category_tree import (
    ParsedCategoryLinks,
    bfs_from_root,
    build_catid_map,
    build_category_tree,
    build_linktarget_map,
    collect_articles_from_categories,
    find_categories_by_regex,
    parse_category_links,
    parse_page_dump,
)


def _page_sql(*rows: tuple) -> io.StringIO:
    """Build fake page.sql INSERT from (id, ns, title) tuples."""
    vals = []
    for pid, ns, title in rows:
        vals.append(
            f"({pid},{ns},'{title}',0,0,0.1,'20240101',NULL,1,1000,'wikitext',NULL)"
        )
    return io.StringIO(f"INSERT INTO `page` VALUES {','.join(vals)};\n")


def _lt_sql(*rows: tuple) -> io.StringIO:
    """Build fake linktarget.sql INSERT from (lt_id, ns, title) tuples."""
    vals = []
    for lt_id, ns, title in rows:
        vals.append(f"({lt_id},{ns},'{title}')")
    return io.StringIO(f"INSERT INTO `linktarget` VALUES {','.join(vals)};\n")


def _catlinks_sql(*rows: tuple) -> io.StringIO:
    """Build fake categorylinks.sql INSERT from (cl_from, cl_target_id, cl_type) tuples."""
    vals = []
    for cl_from, cl_target_id, cl_type in rows:
        vals.append(
            f"({cl_from},'SORTKEY','20240101','','{cl_type}',1,{cl_target_id})"
        )
    return io.StringIO(f"INSERT INTO `categorylinks` VALUES {','.join(vals)};\n")


class TestBuildCatidMap:
    def test_filters_namespace_14(self):
        sql = _page_sql((1, 14, "Art"), (2, 0, "Mona_Lisa"), (3, 14, "Science"))
        result = build_catid_map(sql)
        assert result == {1: "Art", 3: "Science"}
        assert 2 not in result

    def test_empty_input(self):
        assert build_catid_map(io.StringIO("")) == {}


class TestBuildLinktargetMap:
    def test_filters_namespace_14(self):
        sql = _lt_sql((100, 14, "Art"), (101, 0, "Article"), (102, 14, "Science"))
        result = build_linktarget_map(sql)
        assert result == {100: "Art", 102: "Science"}
        assert 101 not in result

    def test_empty_input(self):
        assert build_linktarget_map(io.StringIO("")) == {}


class TestBuildCategoryTree:
    def test_simple_tree(self):
        # catid: page_id -> cat name (for resolving cl_from on subcats)
        catid = {10: "SubCat"}
        # lt: lt_id -> cat name (for resolving cl_target_id)
        lt = {500: "Root", 501: "SubCat"}
        catlinks = _catlinks_sql(
            (10, 500, "subcat"),   # SubCat(page_id=10) is subcat of Root(lt_id=500)
            (100, 500, "page"),    # page 100 in Root
            (101, 501, "page"),    # page 101 in SubCat
        )
        tree = build_category_tree(catlinks, "Root", catid, lt_to_name=lt)
        assert "SubCat" in tree.subcategories.get("Root", set())
        assert 100 in tree.article_ids
        assert 101 in tree.article_ids

    def test_deep_tree(self):
        catid = {10: "L1", 11: "L2"}
        lt = {500: "Root", 501: "L1", 502: "L2"}
        catlinks = _catlinks_sql(
            (10, 500, "subcat"),   # L1 under Root
            (11, 501, "subcat"),   # L2 under L1
            (200, 502, "page"),    # page 200 in L2
        )
        tree = build_category_tree(catlinks, "Root", catid, lt_to_name=lt)
        assert 200 in tree.article_ids

    def test_cycle_protection(self):
        catid = {10: "A", 11: "B"}
        lt = {500: "Root", 501: "A", 502: "B"}
        catlinks = _catlinks_sql(
            (10, 500, "subcat"),   # A under Root
            (11, 501, "subcat"),   # B under A
            (10, 502, "subcat"),   # A under B (cycle!)
            (300, 501, "page"),
        )
        tree = build_category_tree(catlinks, "Root", catid, lt_to_name=lt)
        assert 300 in tree.article_ids

    def test_empty_root(self):
        tree = build_category_tree(io.StringIO(""), "Nonexistent", {}, lt_to_name={})
        assert tree.article_ids == set()
        assert tree.subcategories == {}

    def test_catid_resolution(self):
        catid = {10: "Known"}
        lt = {500: "Root", 501: "Known"}
        catlinks = _catlinks_sql(
            (10, 500, "subcat"),   # Known under Root
            (99, 500, "subcat"),   # unknown page_id — skipped
            (200, 501, "page"),
        )
        tree = build_category_tree(catlinks, "Root", catid, lt_to_name=lt)
        assert "Known" in tree.subcategories.get("Root", set())
        assert 200 in tree.article_ids

    def test_file_type_ignored(self):
        catid = {}
        lt = {500: "Root"}
        catlinks = _catlinks_sql(
            (51, 500, "file"),
            (52, 500, "page"),
        )
        tree = build_category_tree(catlinks, "Root", catid, lt_to_name=lt)
        assert 51 not in tree.article_ids
        assert 52 in tree.article_ids

    def test_missing_root_category(self):
        catid = {10: "Other"}
        lt = {500: "Other"}
        catlinks = _catlinks_sql((100, 500, "page"),)
        tree = build_category_tree(catlinks, "Missing", catid, lt_to_name=lt)
        assert tree.article_ids == set()

    def test_backward_compat_no_linktarget(self):
        """Without lt_to_name, no target_ids resolve so tree is empty."""
        catid = {10: "Sub"}
        catlinks = _catlinks_sql((10, 500, "subcat"),)
        tree = build_category_tree(catlinks, "Root", catid)
        assert tree.subcategories == {}


class TestParsePageDump:
    def test_separates_categories_and_articles(self):
        sql = _page_sql(
            (1, 14, "Art"),
            (2, 0, "Mona_Lisa"),
            (3, 14, "Science"),
            (4, 0, "Physics"),
        )
        catid_map, page_meta = parse_page_dump(sql)
        assert catid_map == {1: "Art", 3: "Science"}
        assert 2 in page_meta
        assert 4 in page_meta
        assert page_meta[2] == ("Mona_Lisa", 1000)

    def test_excludes_redirects(self):
        # page_sql helper: (id, ns, title) — is_redirect is always 0
        # Build manually to test redirect exclusion
        vals = "(1,0,'Real',0,0,0.1,'20240101',NULL,1,5000,'wikitext',NULL)"
        vals += ",(2,0,'Redir',1,0,0.1,'20240101',NULL,1,5000,'wikitext',NULL)"
        sql = io.StringIO(f"INSERT INTO `page` VALUES {vals};\n")
        _, page_meta = parse_page_dump(sql)
        assert 1 in page_meta
        assert 2 not in page_meta

    def test_ignores_non_article_namespaces(self):
        sql = _page_sql(
            (1, 0, "Article"),
            (2, 1, "Talk_page"),
            (3, 14, "Category"),
        )
        _, page_meta = parse_page_dump(sql)
        assert 1 in page_meta
        assert 2 not in page_meta  # ns=1 excluded
        assert 3 not in page_meta  # ns=14 goes to catid_map

    def test_empty_input(self):
        catid_map, page_meta = parse_page_dump(io.StringIO(""))
        assert catid_map == {}
        assert page_meta == {}

    def test_page_meta_has_length(self):
        sql = _page_sql((42, 0, "Test_Article"),)
        _, page_meta = parse_page_dump(sql)
        assert page_meta[42][1] == 1000  # default length in _page_sql


class TestParseCategoryLinks:
    def test_builds_full_adjacency(self):
        catid = {10: "SubA", 11: "SubB"}
        lt = {500: "Root", 501: "SubA"}
        catlinks = _catlinks_sql(
            (10, 500, "subcat"),
            (11, 500, "subcat"),
            (100, 500, "page"),
            (101, 501, "page"),
        )
        parsed = parse_category_links(catlinks, catid, lt_to_name=lt)
        assert parsed.children["Root"] == {"SubA", "SubB"}
        assert 100 in parsed.cat_pages["Root"]
        assert 101 in parsed.cat_pages["SubA"]

    def test_empty_input(self):
        parsed = parse_category_links(io.StringIO(""), {}, lt_to_name={})
        assert parsed.children == {}
        assert parsed.cat_pages == {}

    def test_no_lt_map(self):
        catid = {10: "Sub"}
        catlinks = _catlinks_sql((10, 500, "subcat"),)
        parsed = parse_category_links(catlinks, catid)
        assert parsed.children == {}

    def test_file_type_skipped(self):
        lt = {500: "Root"}
        catlinks = _catlinks_sql((51, 500, "file"),)
        parsed = parse_category_links(catlinks, {}, lt_to_name=lt)
        assert 51 not in parsed.cat_pages.get("Root", set())


class TestBfsFromRoot:
    def test_simple_bfs(self):
        parsed = ParsedCategoryLinks(
            children={"Root": {"A", "B"}, "A": {"C"}},
            cat_pages={"Root": {1}, "A": {2}, "C": {3}},
        )
        tree = bfs_from_root(parsed, "Root")
        assert tree.article_ids == {1, 2, 3}
        assert "A" in tree.subcategories["Root"]

    def test_cycle_protection(self):
        parsed = ParsedCategoryLinks(
            children={"A": {"B"}, "B": {"A"}},
            cat_pages={"A": {1}, "B": {2}},
        )
        tree = bfs_from_root(parsed, "A")
        assert tree.article_ids == {1, 2}

    def test_empty_root(self):
        parsed = ParsedCategoryLinks()
        tree = bfs_from_root(parsed, "Missing")
        assert tree.article_ids == set()
        assert tree.subcategories == {}

    def test_max_depth_zero_root_only(self):
        parsed = ParsedCategoryLinks(
            children={"Root": {"A"}, "A": {"B"}},
            cat_pages={"Root": {1}, "A": {2}, "B": {3}},
        )
        tree = bfs_from_root(parsed, "Root", max_depth=0)
        assert tree.article_ids == {1}
        assert tree.subcategories == {}

    def test_max_depth_limits_traversal(self):
        parsed = ParsedCategoryLinks(
            children={"Root": {"L1"}, "L1": {"L2"}, "L2": {"L3"}},
            cat_pages={"Root": {1}, "L1": {2}, "L2": {3}, "L3": {4}},
        )
        tree = bfs_from_root(parsed, "Root", max_depth=1)
        assert tree.article_ids == {1, 2}
        assert 3 not in tree.article_ids
        assert 4 not in tree.article_ids

    def test_max_depth_none_unlimited(self):
        parsed = ParsedCategoryLinks(
            children={"Root": {"L1"}, "L1": {"L2"}, "L2": {"L3"}},
            cat_pages={"Root": {1}, "L1": {2}, "L2": {3}, "L3": {4}},
        )
        tree = bfs_from_root(parsed, "Root", max_depth=None)
        assert tree.article_ids == {1, 2, 3, 4}

    def test_depth_stats_populated(self):
        parsed = ParsedCategoryLinks(
            children={"Root": {"A", "B"}, "A": {"C"}},
            cat_pages={"Root": {1}, "A": {2, 3}, "B": {4}, "C": {5, 6}},
        )
        tree = bfs_from_root(parsed, "Root")
        assert len(tree.depth_stats) == 3  # depths 0, 1, 2
        # depth 0: 1 cat (Root), 1 article
        assert tree.depth_stats[0] == (1, 1)
        # depth 1: 2 cats (A, B), 3 articles (2, 3, 4)
        assert tree.depth_stats[1] == (2, 3)
        # depth 2: 1 cat (C), 2 articles (5, 6)
        assert tree.depth_stats[2] == (1, 2)

    def test_depth_stats_empty_root(self):
        parsed = ParsedCategoryLinks()
        tree = bfs_from_root(parsed, "Missing")
        assert tree.depth_stats == [(1, 0)]  # root visited, no articles

    def test_depth_stats_deduplicates_articles(self):
        parsed = ParsedCategoryLinks(
            children={"Root": {"A"}},
            cat_pages={"Root": {1, 2}, "A": {2, 3}},  # article 2 in both
        )
        tree = bfs_from_root(parsed, "Root")
        assert tree.article_ids == {1, 2, 3}
        # depth 0: 2 new articles (1, 2); depth 1: 1 new article (3, since 2 already counted)
        assert tree.depth_stats[0] == (1, 2)
        assert tree.depth_stats[1] == (1, 1)

    def test_build_category_tree_wrapper_matches(self):
        """build_category_tree (wrapper) matches parse + bfs."""
        catid = {10: "SubCat"}
        lt = {500: "Root", 501: "SubCat"}
        catlinks_data = _catlinks_sql(
            (10, 500, "subcat"),
            (100, 500, "page"),
            (101, 501, "page"),
        )
        tree_wrapper = build_category_tree(catlinks_data, "Root", catid, lt_to_name=lt)

        catlinks_data2 = _catlinks_sql(
            (10, 500, "subcat"),
            (100, 500, "page"),
            (101, 501, "page"),
        )
        parsed = parse_category_links(catlinks_data2, catid, lt_to_name=lt)
        tree_split = bfs_from_root(parsed, "Root")

        assert tree_wrapper.article_ids == tree_split.article_ids
        assert tree_wrapper.subcategories == tree_split.subcategories


class TestFindCategoriesByRegex:
    def test_single_pattern_match(self):
        names = ["French_painters", "Italian_sculptors", "German_poets", "French_writers"]
        result = find_categories_by_regex(names, [r"French"])
        assert result == {"French_painters", "French_writers"}

    def test_multiple_patterns(self):
        names = ["French_painters", "Italian_sculptors", "German_poets"]
        result = find_categories_by_regex(names, [r"paint", r"sculpt"])
        assert result == {"French_painters", "Italian_sculptors"}

    def test_case_insensitive(self):
        names = ["French_Painters", "ITALIAN_PAINTERS"]
        result = find_categories_by_regex(names, [r"painters"])
        assert result == {"French_Painters", "ITALIAN_PAINTERS"}

    def test_no_match(self):
        names = ["French_painters", "Italian_sculptors"]
        result = find_categories_by_regex(names, [r"musicians"])
        assert result == set()

    def test_empty_names(self):
        result = find_categories_by_regex([], [r"paint"])
        assert result == set()

    def test_empty_patterns(self):
        result = find_categories_by_regex(["French_painters"], [])
        assert result == set()

    def test_regex_special_chars(self):
        names = ["20th-century_painters", "19th-century_painters", "Painters_born_in_1900"]
        result = find_categories_by_regex(names, [r"\d+th-century.*painter"])
        assert result == {"20th-century_painters", "19th-century_painters"}

    def test_anchored_pattern(self):
        names = ["French_painters", "Painters_of_France"]
        result = find_categories_by_regex(names, [r"^French"])
        assert result == {"French_painters"}

    def test_category_matched_once(self):
        """A category matching multiple patterns is only included once."""
        names = ["French_painters"]
        result = find_categories_by_regex(names, [r"French", r"painter"])
        assert result == {"French_painters"}


class TestCollectArticlesFromCategories:
    def test_collects_from_multiple_categories(self):
        parsed = ParsedCategoryLinks(
            cat_pages={"CatA": {1, 2}, "CatB": {3, 4}, "CatC": {5}},
        )
        result = collect_articles_from_categories(parsed, {"CatA", "CatB"})
        assert result == {1, 2, 3, 4}

    def test_deduplicates_across_categories(self):
        parsed = ParsedCategoryLinks(
            cat_pages={"CatA": {1, 2}, "CatB": {2, 3}},
        )
        result = collect_articles_from_categories(parsed, {"CatA", "CatB"})
        assert result == {1, 2, 3}

    def test_missing_category_ignored(self):
        parsed = ParsedCategoryLinks(
            cat_pages={"CatA": {1}},
        )
        result = collect_articles_from_categories(parsed, {"CatA", "Missing"})
        assert result == {1}

    def test_empty_categories(self):
        parsed = ParsedCategoryLinks()
        result = collect_articles_from_categories(parsed, set())
        assert result == set()

    def test_no_articles_in_matched_category(self):
        parsed = ParsedCategoryLinks(
            children={"CatA": {"CatB"}},
            cat_pages={},
        )
        result = collect_articles_from_categories(parsed, {"CatA"})
        assert result == set()
