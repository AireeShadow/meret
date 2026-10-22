"""The tag tree lists off the GUI thread and survives a rebuild mid-flight."""

import pytest

from tests.gui.conftest import pump

pytest.importorskip("PySide6")


@pytest.fixture
def tagged(tmp_path):
    """A repository with a nested tag pair, so a row has something to expand into."""
    from meret.core.sqlite_repo import SQLiteRepository

    repo = SQLiteRepository(str(tmp_path / "meret.db"))
    repo.create_tag("travel")
    repo.create_tag("travel/beach")
    return repo


@pytest.fixture
def window(tmp_path, qapp, qtbot, tagged):
    from meret.gui.app import MainWindow

    main = MainWindow(tagged, tmp_path / "meret.db")
    qtbot.addWidget(main)
    qtbot.waitUntil(lambda: main.tags.topLevelItemCount() == 1, timeout=2000)
    return main


def test_root_fills_without_blocking(window):
    assert window.tags.topLevelItem(0).text(0) == "travel"


def test_tag_listing_runs_off_the_gui_thread(tmp_path, qapp, qtbot, tagged):
    """listdir can rebuild the snapshot: on the GUI thread that is a frozen window."""
    import threading

    from meret.gui.app import MainWindow

    main = MainWindow(tagged, tmp_path / "meret.db")
    qtbot.addWidget(main)

    seen = []
    original = main._tree.listdir
    main._tree.listdir = lambda vpath="/": (
        seen.append(threading.current_thread()),
        original(vpath),
    )[1]

    main.tags.reload()
    qtbot.waitUntil(lambda: bool(seen), timeout=2000)

    assert seen[0] is not threading.main_thread()


def test_expanding_shows_a_placeholder_then_the_children(window):
    """The row must not look empty while the listing is on its way."""
    row = window.tags.topLevelItem(0)

    window.tags.expandItem(row)
    # delivery is a queued call, so right here nothing can have landed yet
    assert row.childCount() == 1
    assert row.child(0).text(0) == "…"

    qtbot_wait = window.tags
    pump(20)
    assert row.child(0).text(0) == "beach"


def test_reload_while_a_listing_is_in_flight(window):
    """The callback must not touch a row that the rebuild already deleted."""
    window.tags.expandItem(window.tags.topLevelItem(0))
    window.tags.reload()

    pump(50)
    assert window.tags.topLevelItemCount() == 1
    assert window.tags.topLevelItem(0).text(0) == "travel"


def test_a_failed_listing_is_visible(tmp_path, qapp, qtbot, tagged):
    """A row that failed says so instead of staying empty."""
    from meret.gui.app import MainWindow
    from meret.vfs.errors import NotFound

    main = MainWindow(tagged, tmp_path / "meret.db")
    qtbot.addWidget(main)

    def boom(vpath="/"):
        raise NotFound(vpath)

    main._tree.listdir = boom
    main.tags.reload()
    pump(50)

    assert main.tags.topLevelItemCount() == 1
    shown = main.tags.topLevelItem(0).text(0)
    assert shown and shown != "…"