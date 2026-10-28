"""Switching database must not freeze the window: opening and wiping both retry.

A retry sleeps up to half a second while a task in flight holds the file, so the
work belongs on the pool and only the swap stays on the GUI thread.
"""

import sqlite3

import pytest

from tests.gui.conftest import pump_until

pytest.importorskip("PySide6")


@pytest.fixture
def window(tmp_path, qapp, qtbot):
    from meret.core.sqlite_repo import SQLiteRepository
    from meret.gui.app import MainWindow

    repo = SQLiteRepository(str(tmp_path / "meret.db"))
    main = MainWindow(repo, tmp_path / "meret.db")
    qtbot.addWidget(main)
    # The start-up listing is still in flight, and its callback ends in
    # _update_status(): it would overwrite a status line written before it lands.
    #
    # pump_until, not qtbot.waitUntil. Revealing the database folder expands its
    # ancestors, and %TEMP% - an ancestor of every tmp_path - is resolved on the
    # GUI thread with a stat per entry, so a single such event can outlast the
    # whole timeout. qtbot.waitUntil re-checks the clock before it re-checks the
    # callback, so a delivery that lands during that event is reported as a
    # timeout. pump_until evaluates the predicate once more after its last pump,
    # which is exactly the turn the delivery needs.
    assert pump_until(lambda: not main._tasks._inflight, timeout_ms=15000), (
        "the start-up listing never landed - the GUI thread is probably stuck in a "
        "filesystem listing"
    )
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