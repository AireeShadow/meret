"""The sqlite3.Row quirk that silently untagged every file.

Row looks like a dict and even has keys(), but `in` tests values. SIM118 "fixed"
`key in row.keys()` into `key in row` and `_tags_from_row` started returning [] for
every row, so the whole repository contract went red. These are the three cases.
"""

import sqlite3

import pytest

from meret.core.sqlite_repo import _TAG_SEP, SQLiteRepository


@pytest.fixture
def conn():
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    yield connection
    connection.close()


def test_in_on_a_row_tests_values_not_column_names(conn):
    """The trap itself: `in row` is about values, so a column name is not "in" it.

    keys() is held in a local, exactly as _tags_from_row does it: writing
    `"other" in row.keys()` would invite SIM118 to "fix" it back into the trap.
    """
    row = conn.execute("SELECT 'tag_names' AS tag_names, 2 AS other").fetchone()
    columns = row.keys()

    assert columns == ["tag_names", "other"]
    assert "tag_names" in row  # True only because a VALUE happens to equal it
    assert "other" not in row  # a column name that is not a value
    assert "other" in columns


def test_no_tag_names_column_means_no_tags(conn):
    """A query without tag_names must not explode and must not invent tags."""
    row = conn.execute("SELECT 1 AS id").fetchone()

    assert SQLiteRepository._tags_from_row(row) == []


def test_tag_names_are_split_on_the_separator(conn):
    """GROUP_CONCAT packs the names with \\x1f, and the split has to undo it."""
    row = conn.execute("SELECT ? AS tag_names", (f"a{_TAG_SEP}b",)).fetchone()

    assert SQLiteRepository._tags_from_row(row) == ["a", "b"]


def test_an_empty_group_concat_is_no_tags(conn):
    """COALESCE(GROUP_CONCAT(...), '') is '' when the file has no tags at all."""
    row = conn.execute("SELECT '' AS tag_names").fetchone()

    assert SQLiteRepository._tags_from_row(row) == []