"""Tests for sql_parser — streaming MySQL INSERT parser."""

from __future__ import annotations

import io

from wiki_pipeline.sql_parser import iter_rows


def _lines(text: str) -> io.StringIO:
    return io.StringIO(text)


class TestIterRows:
    def test_empty_file(self):
        assert list(iter_rows(_lines(""), "page")) == []

    def test_no_insert_lines(self):
        sql = "-- comment\nCREATE TABLE `page` (id int);\n"
        assert list(iter_rows(_lines(sql), "page")) == []

    def test_single_row(self):
        sql = "INSERT INTO `t` VALUES (1,'hello',3);\n"
        rows = list(iter_rows(_lines(sql), "t"))
        assert rows == [("1", "hello", "3")]

    def test_multiple_rows_one_line(self):
        sql = "INSERT INTO `t` VALUES (1,'a'),(2,'b'),(3,'c');\n"
        rows = list(iter_rows(_lines(sql), "t"))
        assert len(rows) == 3
        assert rows[0] == ("1", "a")
        assert rows[2] == ("3", "c")

    def test_escaped_single_quote(self):
        sql = "INSERT INTO `t` VALUES (1,'it\\'s');\n"
        rows = list(iter_rows(_lines(sql), "t"))
        assert rows == [("1", "it's")]

    def test_escaped_backslash(self):
        sql = "INSERT INTO `t` VALUES (1,'path\\\\dir');\n"
        rows = list(iter_rows(_lines(sql), "t"))
        assert rows == [("1", "path\\dir")]

    def test_null_values(self):
        sql = "INSERT INTO `t` VALUES (1,NULL,'x',NULL);\n"
        rows = list(iter_rows(_lines(sql), "t"))
        assert rows == [("1", None, "x", None)]

    def test_comma_in_string(self):
        sql = "INSERT INTO `t` VALUES (1,'hello, world');\n"
        rows = list(iter_rows(_lines(sql), "t"))
        assert rows == [("1", "hello, world")]

    def test_parentheses_in_string(self):
        sql = "INSERT INTO `t` VALUES (1,'a(b)c');\n"
        rows = list(iter_rows(_lines(sql), "t"))
        assert rows == [("1", "a(b)c")]

    def test_wrong_table_name_ignored(self):
        sql = "INSERT INTO `other` VALUES (1,'x');\n"
        assert list(iter_rows(_lines(sql), "page")) == []

    def test_multiple_insert_lines(self):
        sql = (
            "INSERT INTO `t` VALUES (1,'a');\n"
            "INSERT INTO `t` VALUES (2,'b');\n"
        )
        rows = list(iter_rows(_lines(sql), "t"))
        assert len(rows) == 2
        assert rows[0] == ("1", "a")
        assert rows[1] == ("2", "b")

    def test_mixed_lines(self):
        sql = (
            "-- comment\n"
            "INSERT INTO `t` VALUES (1,'a');\n"
            "ALTER TABLE `t`;\n"
            "INSERT INTO `t` VALUES (2,'b');\n"
        )
        rows = list(iter_rows(_lines(sql), "t"))
        assert len(rows) == 2
