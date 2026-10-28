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


def test_revealing_the_parent_walks_up_one_level(window, tmp_path):
    """Up is gone as a command: the panel walks by revealing, not by re-rooting."""
    deep = tmp_path / "a" / "b"
    deep.mkdir(parents=True)
    assert window.fs.reveal(deep)
    assert pump_until(lambda: window.fs.current_path() == deep)

    assert window.fs.reveal(deep.parent)

    assert pump_until(lambda: window.fs.current_path() == tmp_path / "a")


def test_the_top_of_a_drive_is_where_walking_up_stops(window, tmp_path):
    """pathlib hands back the drive itself, so the drive list is the last stop."""
    top = Path(tmp_path.anchor)
    assert window.fs.reveal(top)
    assert pump_until(lambda: window.fs.current_path() == top)

    current = window.fs.current_path()
    assert current.parent == current

    window.fs.show_filesystem()

    assert not window.fs.rootIndex().isValid()
    assert pump_until(lambda: _top_count(window.fs) > 0)


def test_the_panel_does_not_watch_the_filesystem(panel):
    """Watching every listed directory re-reads it on the GUI thread - seconds."""
    from PySide6.QtWidgets import QFileSystemModel

    assert panel._model.testOption(QFileSystemModel.Option.DontWatchForChanges)
    assert panel._model.testOption(QFileSystemModel.Option.DontResolveSymlinks)
    assert panel._model.testOption(QFileSystemModel.Option.DontUseCustomDirectoryIcons)


def test_refresh_leaves_the_panel_rooted_at_the_filesystem_top(panel):
    """The panel is a view of everything, and refresh() must not re-root it."""
    panel.refresh()

    assert panel._model.rootPath() == ""
    assert not panel.rootIndex().isValid()


def test_the_panel_waits_for_the_drive_list_before_checking_the_root(panel):
    """The listing arrives from Qt's own thread: the root must survive the wait."""
    assert pump_until(lambda: panel._model.rowCount() > 0)

    assert panel._model.rootPath() == ""
    assert not panel.rootIndex().isValid()


def test_revealing_a_folder_leaves_the_panel_at_the_top(panel, tmp_path):
    """Being left rooted at the revealed folder is what hid the other drives."""
    panel.reveal(tmp_path)

    assert panel._model.rootPath() == ""
    assert not panel.rootIndex().isValid()


def test_reveal_does_not_list_a_path_it_already_knows(panel, tmp_path):
    """Every listing is resolved on the GUI thread: a known path must not be re-read."""
    assert panel.reveal(tmp_path)

    calls = []
    panel._model.setRootPath = lambda *args: calls.append(args)

    assert panel.reveal(tmp_path)
    assert calls == []


def test_the_drives_stay_the_root_of_the_panel(panel):
    """The drive list is the model's root: the panel must sit at it, not inside it."""
    assert pump_until(lambda: panel._model.rowCount() > 0)

    assert panel._model.rootPath() == ""
    assert not panel.rootIndex().isValid()

def test_show_filesystem_reads_the_top_again(panel):
    """A no-op setRootPath("") returns early: the root has to be forced to re-read.

    The bug: the panel reported a single row instead of the drive list, because the
    root was never read at all - index() was valid, so nothing re-rooted the model.
    """
    from PySide6.QtCore import QDir

    calls = []
    panel._model.setRootPath = lambda path: calls.append(path)

    panel.show_filesystem()

    assert calls == [QDir.homePath(), ""]

def test_reveal_does_not_list_a_path_it_already_knows(panel, tmp_path):
    """A known path must not be listed again: only the way back to the top moves.

    _restore_top() is not a listing of the target - it steps through the home folder
    and back, which is the only way to get the model's own root read at all.
    """
    from PySide6.QtCore import QDir

    assert panel.reveal(tmp_path)

    calls = []
    panel._model.setRootPath = lambda *args: calls.append(args)

    assert panel.reveal(tmp_path)

    assert calls == [(QDir.homePath(),), ("",)]  # the top, never the target

# ---------- drives: a dead one must not hide the others ----------


def _top_paths(panel) -> list[str]:
    model = panel.model()
    return [model.filePath(model.index(r, 0, QModelIndex())) for r in range(_top_count(panel))]


def _a_real_drive() -> str:
    """A drive that exists here: "C:/" on Windows, "/" on Unix."""
    import os

    from PySide6.QtCore import QDir

    return next(d.absolutePath() for d in QDir.drives() if os.path.exists(d.absolutePath()))


def test_a_reachable_drive_gets_its_row_at_once(panel):
    """No waiting for Qt's own drive listing - that is the whole point."""
    drive = _a_real_drive()
    panel._add_drive(drive, True)
    assert drive in _top_paths(panel)


def test_an_unreachable_drive_is_left_to_qt_and_probed_again_later(panel):
    panel._probing.add("Q:/")
    panel._add_drive("Q:/", False)
    assert "Q:/" not in panel._probing
    assert "Q:/" not in _top_paths(panel)


def test_one_probe_per_drive_and_none_twice(panel, monkeypatch):
    """A refresh while a dead drive is still thinking must not stack threads on it."""
    import meret.gui.app as gui
    from PySide6.QtCore import QDir

    started = []

    class FakeThread:
        def __init__(self, target, args, name, daemon):
            started.append(args[0])

        def start(self):
            pass

    monkeypatch.setattr(gui.threading, "Thread", FakeThread)
    panel._probing.clear()
    panel._probe_drives()
    panel._probe_drives()
    assert sorted(started) == sorted(d.absolutePath() for d in QDir.drives())


def test_a_slow_drive_does_not_hold_back_the_others(qapp, qtbot, monkeypatch):
    """The bug: a disconnected network drive took ~21 s to answer, and Qt hands the
    drives over in one batch, so for 21 s only one drive was on screen."""
    import threading

    import meret.gui.app as gui
    from PySide6.QtCore import QDir, QFileInfo

    real = _a_real_drive()
    slow = "Q:/"
    released = threading.Event()

    class TwoDrives(QDir):
        @staticmethod
        def drives():
            return [QFileInfo(real), QFileInfo(slow)]

    def reachable(path):
        if path == slow:
            released.wait(10)  # stands in for Windows giving up on a dead drive
            return False
        return True

    monkeypatch.setattr(gui, "QDir", TwoDrives)
    monkeypatch.setattr(gui.RealFsPanel, "_reachable", staticmethod(reachable))
    panel = gui.RealFsPanel()
    qtbot.addWidget(panel)
    try:
        assert pump_until(lambda: real in _top_paths(panel), timeout_ms=1000)
        assert slow in panel._probing  # still thinking - and nobody waited for it
    finally:
        released.set()
    assert pump_until(lambda: slow not in panel._probing, timeout_ms=2000)


def test_the_top_is_sorted_by_name_ascending(panel):
    """setSortingEnabled() alone sorted descending: drives from Z down to C."""
    from PySide6.QtCore import Qt

    assert panel.header().sortIndicatorOrder() == Qt.SortOrder.AscendingOrder
    assert panel.header().sortIndicatorSection() == 0
