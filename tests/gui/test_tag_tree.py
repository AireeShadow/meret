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


def _break_every_listing(main, error: Exception | None = None) -> list[str]:
    """Fail every listing. The row that carries the failure is the tree's root."""
    from meret.vfs.errors import NotFound

    calls: list[str] = []

    def boom(vpath: str = "/"):
        calls.append(vpath)
        raise error or NotFound(vpath)

    main._tree.listdir = boom
    return calls


def _break_one_folder(main, vpath: str = "/travel") -> list[str]:
    """Fail only `vpath`, and log the calls that reached it.

    The root itself is the invisible item, so a failure there lands on the tree, not
    on a row: FILLED and LOADING can only be read off a real folder such as "travel".
    """
    from meret.vfs.errors import NotFound

    calls: list[str] = []
    original = main._tree.listdir

    def boom(path: str = "/"):
        if path == vpath:
            calls.append(path)
            raise NotFound(path)
        return original(path)

    main._tree.listdir = boom
    return calls


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
    """A listing that failed says so instead of staying empty."""
    from meret.gui.app import MainWindow

    main = MainWindow(tagged, tmp_path / "meret.db")
    qtbot.addWidget(main)
    _break_every_listing(main)
    main.tags.reload()
    pump(50)

    assert main.tags.topLevelItemCount() == 1
    shown = main.tags.topLevelItem(0).text(0)
    assert shown and shown != "…"


def test_a_failed_row_is_marked_as_filled(window):
    """FILLED is what keeps the next expand from queueing the same broken listing."""
    from meret.gui.app import TagTree

    row = window.tags.topLevelItem(0)
    calls = _break_one_folder(window)

    window.tags.expandItem(row)
    pump(50)

    assert len(calls) == 1
    assert row.data(0, TagTree.FILLED) is True
    assert row.data(0, TagTree.LOADING) is False


def test_a_failed_row_keeps_its_message_on_collapse_and_expand(window):
    """A second expand must not blank the row out for a retry nobody asked for."""
    row = window.tags.topLevelItem(0)
    calls = _break_one_folder(window)

    window.tags.expandItem(row)
    pump(50)

    shown = row.child(0).text(0)
    assert shown and shown != "…"

    window.tags.collapseItem(row)
    window.tags.expandItem(row)
    pump(50)

    assert len(calls) == 1  # FILLED: no new listing
    assert row.childCount() == 1
    assert row.child(0).text(0) == shown


def test_reload_is_what_retries_a_failed_row(window):
    """F5 is the retry: it rebuilds the tree, so the row is queued again."""
    row = window.tags.topLevelItem(0)
    calls = _break_one_folder(window)

    window.tags.expandItem(row)
    pump(50)
    assert len(calls) == 1

    window.tags.reload()
    pump(50)
    window.tags.expandItem(window.tags.topLevelItem(0))
    pump(50)

    assert len(calls) == 2