"""The real-FS panel covers the whole filesystem, not the folder it started in.

Qt populates QFileSystemModel on its own thread, so rowCount() is 0 until the
listing lands: every check here waits instead of asserting straight away.
"""

from pathlib import Path

import pytest

from tests.gui.conftest import pump_until

pytest.importorskip("PySide6")

from PySide6.QtCore import QModelIndex


@pytest.fixture
def panel(qapp, qtbot):
    from meret.gui.fs_panel import RealFsPanel

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


def test_path_of_an_invalid_index_is_none(panel):
    assert panel.path_of(QModelIndex()) is None


def test_revealing_the_parent_walks_up_one_level(window, tmp_path):
    """Up is gone as a command: the panel walks by revealing, not by re-rooting."""
    deep = tmp_path / "a" / "b"
    deep.mkdir(parents=True)
    assert window.fs.reveal(deep)
    assert pump_until(lambda: window.fs.current_path() == deep)

    assert window.fs.reveal(deep.parent)

    assert pump_until(lambda: window.fs.current_path() == tmp_path / "a")


def test_the_top_of_a_drive_is_where_walking_up_stops(window, tmp_path):
    """pathlib hands back the drive itself, so the drive is the last stop."""
    top = Path(tmp_path.anchor)
    assert top.parent == top  # nothing above a drive

    assert window.fs.reveal(top)
    # ...and the panel goes back to its own root afterwards, never to the drive list
    assert window.fs._model.rootPath() == window.fs._root_path


def test_the_panel_does_not_watch_the_filesystem(panel):
    """Watching every listed directory re-reads it on the GUI thread - seconds."""
    from PySide6.QtWidgets import QFileSystemModel

    assert panel._model.testOption(QFileSystemModel.Option.DontWatchForChanges)
    assert panel._model.testOption(QFileSystemModel.Option.DontResolveSymlinks)
    assert panel._model.testOption(QFileSystemModel.Option.DontUseCustomDirectoryIcons)


# ---------- drives: a dead one must not hide the others ----------


def _top_paths(panel) -> list[str]:
    model = panel.model()
    return [model.filePath(model.index(r, 0, QModelIndex())) for r in range(_top_count(panel))]


def _a_real_drive() -> str:
    """A drive that exists here: "C:/" on Windows, "/" on Unix."""
    import os

    from PySide6.QtCore import QDir

    return next(d.absolutePath() for d in QDir.drives() if os.path.exists(d.absolutePath()))


def test_the_top_is_sorted_by_name_ascending(panel):
    """setSortingEnabled() alone sorted descending: drives from Z down to C."""
    from PySide6.QtCore import Qt

    assert panel.header().sortIndicatorOrder() == Qt.SortOrder.AscendingOrder
    assert panel.header().sortIndicatorSection() == 0


def test_the_panel_is_rooted_at_one_folder(panel):
    """The panel shows ONE folder, never the model's own root.

    The model's root is the drive list, and building it asks Windows for every
    drive's label and free space: ~5.5 s per dead mapped drive, on the GUI thread,
    in a single event. Rooting at one folder means it is never built.
    """
    assert panel.rootIndex().isValid()
    assert panel._model.rootPath() == panel._root_path


def test_reveal_a_missing_path_fails_quietly(panel, tmp_path):
    """setRootPath() returns an index even for a path that is not there."""
    assert panel.reveal(tmp_path / "nope") is False
    assert panel._model.rootPath() == panel._root_path


def test_the_window_does_not_reveal_the_database_folder_on_start(window):
    """reveal() expands every ancestor with a stat per entry - %TEMP% alone is seconds.

    The window logs the database folder and roots the panel at the home folder
    instead, so start-up lists none of the database's ancestors.
    """
    from PySide6.QtCore import QDir

    assert window.fs._model.rootPath() == QDir.homePath()
    assert window.fs.current_path() != window._db_path.parent


def test_refresh_keeps_the_panel_at_its_root(panel):
    """The panel is a view of one folder, and refresh() must not re-root it."""
    panel.refresh()

    assert panel._model.rootPath() == panel._root_path
    assert panel.rootIndex().isValid()


def test_the_root_survives_the_first_listing(panel):
    """The listing arrives from Qt's own thread: the root must survive the wait."""
    assert pump_until(lambda: panel._model.rowCount(panel.rootIndex()) > 0)

    assert panel._model.rootPath() == panel._root_path


def test_revealing_a_folder_leaves_the_panel_at_its_root(panel, tmp_path):
    """Being left rooted at the revealed folder is what hid every other folder."""
    panel.reveal(tmp_path)

    assert panel._model.rootPath() == panel._root_path


def test_the_panel_never_asks_for_the_drive_list(panel):
    """The drive list is the freeze: building it stats every drive on the GUI thread.

    This is the regression guard. `""` is the model's own root, so any call carrying
    it means the panel is about to enumerate drives - 5.5 s per dead mapped drive,
    and the whole reason the panel was re-rooted.
    """
    from PySide6.QtCore import QDir

    calls = []
    original = panel._model.setRootPath
    panel._model.setRootPath = lambda path: calls.append(path) or original(path)

    panel.refresh()
    panel.show_filesystem()

    assert "" not in calls
    assert panel._model.rootPath() == QDir.homePath()


def test_show_filesystem_steps_through_the_parent_and_never_the_drive_list(panel):
    """A no-op setRootPath() returns early: the parent step is what forces a re-read."""
    calls = []
    original = panel._model.setRootPath
    panel._model.setRootPath = lambda path: calls.append(path) or original(path)

    panel.show_filesystem()

    assert calls == [str(Path(panel._root_path).parent), panel._root_path]


def test_reveal_does_not_re_root_a_path_it_already_knows(panel):
    """A known path is not listed again: only the step back to the root moves."""
    inside = Path(panel._root_path)
    assert panel.reveal(inside)

    calls = []
    original = panel._model.setRootPath
    panel._model.setRootPath = lambda path: calls.append(path) or original(path)

    assert panel.reveal(inside)

    assert calls == [panel._root_path]
