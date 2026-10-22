"""The real-FS panel covers the whole filesystem, not the folder it started in.

Qt populates QFileSystemModel on its own thread, so rowCount() is 0 until the
listing lands: every check here waits instead of asserting straight away.
"""

from pathlib import Path

import pytest

from tests.gui.conftest import pump_until

pytest.importorskip("PySide6")

from PySide6.QtCore import QModelIndex  # noqa: E402


@pytest.fixture
def panel(qapp, qtbot):
    from meret.gui.app import RealFsPanel

    widget = RealFsPanel()
    qtbot.addWidget(widget)
    return widget


@pytest.fixture
def window(tmp_path, qapp, qtbot):
    from meret.core.sqlite_repo import SQLiteRepository
    from meret.gui.app import MainWindow

    repo = SQLiteRepository(str(tmp_path / "meret.db"))
    main = MainWindow(repo, tmp_path / "meret.db")
    qtbot.addWidget(main)
    return main


def _top_count(panel) -> int:
    return panel.model().rowCount(QModelIndex())


def test_the_panel_is_rooted_at_the_filesystem_top(panel):
    """The bug: the view was rooted at one folder and showed only its contents."""
    assert not panel.rootIndex().isValid()  # the model's own root: drives or "/"


def test_the_top_row_is_a_filesystem_root(panel):
    assert pump_until(lambda: _top_count(panel) > 0)
    top = panel.path_of(panel.model().index(0, 0, QModelIndex()))
    assert top is not None
    assert top.parent == top  # "/" on Unix, "C:\\" on Windows


def test_reveal_selects_the_folder_and_expands_its_parents(panel, tmp_path):
    """A hidden segment must not break it: tmp_path sits under AppData on Windows."""
    deep = tmp_path / "a" / "b"
    deep.mkdir(parents=True)

    assert panel.reveal(deep)
    assert pump_until(lambda: panel.current_path() == deep)

    parent = panel.currentIndex().parent()
    while parent.isValid():
        assert panel.isExpanded(parent)
        parent = parent.parent()


def test_reveal_a_missing_path_fails_quietly(panel, tmp_path):
    assert panel.reveal(tmp_path / "nope") is False


def test_path_of_an_invalid_index_is_none(panel):
    assert panel.path_of(QModelIndex()) is None


def test_the_window_reveals_the_database_folder_on_start(window):
    assert pump_until(lambda: window.fs.current_path() == window._db_path.parent)


def test_up_walks_to_the_parent(window, tmp_path):
    assert window.fs.reveal(tmp_path)

    window._go_up()

    assert window.fs.current_path() == tmp_path.parent


def test_up_at_the_filesystem_top_shows_the_top(window, tmp_path):
    assert window.fs.reveal(Path(tmp_path.anchor))

    window._go_up()

    assert not window.fs.rootIndex().isValid()
    assert pump_until(lambda: _top_count(window.fs) > 0)