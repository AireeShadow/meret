"""Switching database must not freeze the window: opening and wiping both retry.

A retry sleeps up to half a second while a task in flight holds the file, so the
work belongs on the pool and only the swap stays on the GUI thread.
"""

import sqlite3

import pytest

from tests.gui.conftest import pump, pump_until

pytest.importorskip("PySide6")


@pytest.fixture
def window(tmp_path, qapp, qtbot):
    from meret.core.sqlite_repo import SQLiteRepository
    from meret.gui.app import MainWindow

    repo = SQLiteRepository(str(tmp_path / "meret.db"))
    main = MainWindow(repo, tmp_path / "meret.db")
    qtbot.addWidget(main)

    # Two waits, in this order, because the window does two things on start.
    #
    # First the panel's own reveal: it expands every ancestor up to the drive root,
    # and %TEMP% - an ancestor of every tmp_path - is resolved with a stat per entry.
    # On Windows that is tens of seconds of a blocked GUI thread, and everything
    # queued behind it waits, including the listing below. pump_until, not
    # qtbot.waitUntil: the latter re-checks the clock before it re-checks the
    # callback, so a delivery that lands during the block is reported as a timeout.
    assert pump_until(lambda: main.fs.current_path() == main._db_path.parent, timeout_ms=60000), (
        "the start-up reveal never finished - the panel is stuck expanding %TEMP%"
    )

    # Then the start-up listing, whose callback ends in _update_status() and would
    # overwrite a status line the test wrote before it lands. Waiting on the pool
    # itself beats polling a predicate: no guessed budget, and the delivery below is
    # a single pump instead of a race with the next version probe.
    assert main._tasks.wait(60000), "the start-up listing never finished"
    pump(20)
    return main


def test_use_db_hands_control_back_before_the_switch(window, tmp_path):
    """Synchronous would mean the caller waits: the old database is still in place."""
    before = window._db_path

    window._use_db(tmp_path / "other.db")

    assert window._db_path == before


def test_the_new_database_is_opened_off_the_gui_thread(window, tmp_path, qtbot):
    """Opening can sleep on a lock: on the GUI thread that is a frozen window."""
    import threading

    seen = []
    original = window._open_db
    window._open_db = lambda path: (
        seen.append(threading.current_thread()),
        original(path),
    )[1]

    target = tmp_path / "other.db"
    window._use_db(target)
    qtbot.waitUntil(lambda: window._db_path == target, timeout=2000)

    assert seen
    assert seen[0] is not threading.main_thread()


def test_wiping_runs_off_the_gui_thread(window, tmp_path, qtbot):
    """The wipe retries the same way, so it moves for the same reason."""
    import threading

    seen = []
    original = window._wipe
    window._wipe = lambda old, path: (
        seen.append(threading.current_thread()),
        original(old, path),
    )[1]

    target = tmp_path / "other.db"
    window._use_db(target, fresh=True)
    qtbot.waitUntil(lambda: window._db_path == target, timeout=2000)

    assert seen
    assert seen[0] is not threading.main_thread()


def test_open_db_waits_out_a_lock(window, tmp_path, qtbot, monkeypatch):
    """A lock is waited out, not reported: the second or third attempt gets through."""
    from meret.core.sqlite_repo import SQLiteRepository
    from meret.gui import app as app_module

    attempts = []

    class Flaky(SQLiteRepository):
        def __init__(self, path):
            attempts.append(path)
            if len(attempts) < 3:
                raise sqlite3.OperationalError("database is locked")
            super().__init__(path)

    monkeypatch.setattr(app_module, "SQLiteRepository", Flaky)
    target = tmp_path / "other.db"

    window._use_db(target)
    qtbot.waitUntil(lambda: window._db_path == target, timeout=3000)

    assert len(attempts) == 3


def test_a_database_that_will_not_open_is_a_status_line(window, tmp_path, qtbot):
    """A failure keeps the old database and says so instead of half-switching."""
    before = window._db_path

    def boom(_path):
        raise sqlite3.OperationalError("database is locked")

    window._open_db = boom
    window._use_db(tmp_path / "other.db")
    qtbot.waitUntil(lambda: "locked" in window._status_label.text(), timeout=2000)

    assert window._db_path == before
