"""Smoke tests: the window builds, lists a tagged file and closes cleanly."""
import pytest
from PySide6.QtCore import Qt

from tests.gui.conftest import pump

pytest.importorskip("PySide6")


def _repo_with_one_photo(tmp_path):
    """A repository holding a single image tagged 'travel'."""
    from PIL import Image

    from meret.core.models import FileEntry, FileType
    from meret.core.sqlite_repo import SQLiteRepository

    photo = tmp_path / "photo.png"
    Image.new("RGB", (32, 32), "red").save(photo)

    repo = SQLiteRepository(str(tmp_path / "meret.db"))
    file_id = repo.save_file(
        FileEntry(filename=photo.name, filepath=str(photo), type=FileType.IMAGE)
    )
    repo.add_tag(file_id, "travel")
    return repo


def test_window_lists_tagged_files(tmp_path, qapp, qtbot):
    from meret.gui.app import MainWindow

    repo = _repo_with_one_photo(tmp_path)
    window = MainWindow(repo, tmp_path / "meret.db")
    qtbot.addWidget(window)  # keeps it alive, then closes it at teardown

    window.open_folder("/travel")
    qtbot.waitUntil(lambda: window.grid.count() == 1, timeout=2000)

    assert window.grid.item(0).text() == "photo.png"


def test_window_constructs_without_missing_state(tmp_path, qapp, qtbot):
    from meret.core.sqlite_repo import SQLiteRepository
    from meret.gui.app import MainWindow

    repo = SQLiteRepository(str(tmp_path / "meret.db"))
    window = MainWindow(repo, tmp_path / "meret.db")
    qtbot.addWidget(window)

    assert window.grid.count() == 0  # an empty repo has nothing to show


def test_tag_tree_is_filled_on_start(tmp_path, qapp, qtbot):
    from meret.gui.app import MainWindow

    repo = _repo_with_one_photo(tmp_path)
    window = MainWindow(repo, tmp_path / "meret.db")
    qtbot.addWidget(window)

    qtbot.waitUntil(lambda: window.tags.topLevelItemCount() == 1, timeout=2000)

    item = window.tags.topLevelItem(0)
    assert item.text(0) == "travel"
    assert item.data(0, Qt.ItemDataRole.UserRole) == "/travel"


def test_toolbar_exposes_every_command(tmp_path, qapp, qtbot):
    """All of them, not a comfortable subset: the empty tail was the bug."""
    from meret.core.sqlite_repo import SQLiteRepository
    from meret.gui.app import TOOLBAR, MainWindow
    from meret.gui.commands import command_table, short_label

    repo = SQLiteRepository(str(tmp_path / "meret.db"))
    window = MainWindow(repo, tmp_path / "meret.db")
    qtbot.addWidget(window)

    buttons = [action for action in window.toolbar.actions() if not action.isSeparator()]
    assert [action.iconText() for action in buttons] == [
        short_label(command) for command, _ in command_table()
    ]
    assert {command for command in TOOLBAR if command} == {
        command for command, _ in command_table()
    }
    assert all(not action.icon().isNull() for action in buttons)


def test_toolbar_fits_without_an_overflow_menu(tmp_path, qapp, qtbot):
    """Qt hides buttons behind ">>" when they do not fit, and that reads as missing."""
    from meret.core.sqlite_repo import SQLiteRepository
    from meret.gui.app import ICON, TOOLBAR, MainWindow

    repo = SQLiteRepository(str(tmp_path / "meret.db"))
    window = MainWindow(repo, tmp_path / "meret.db")
    qtbot.addWidget(window)
    window.resize(1280, 720)
    window.show()
    qapp.processEvents()

    width = sum(ICON + 12 if command else 10 for command in TOOLBAR)
    assert width < window.width()


def test_every_toolbar_action_has_a_tooltip_and_a_key(tmp_path, qapp, qtbot):
    """The TUI shows its keys in the footer; here they live in the tooltip."""
    from meret.core.sqlite_repo import SQLiteRepository
    from meret.gui.app import MainWindow

    repo = SQLiteRepository(str(tmp_path / "meret.db"))
    window = MainWindow(repo, tmp_path / "meret.db")
    qtbot.addWidget(window)

    for action in window.toolbar.actions():
        if action.isSeparator():
            continue
        assert action.toolTip().startswith(action.text())
        assert action.shortcut().toString()


def test_pump_accepts_app_positionally(qapp):
    pump(qapp)
    pump(10)
    pump(ms=5)


def test_preview_updates_for_the_selected_file(tmp_path, qapp, qtbot):
    """A second definition of _set_preview once swallowed the token argument."""
    from meret.gui.app import MainWindow

    repo = _repo_with_one_photo(tmp_path)
    window = MainWindow(repo, tmp_path / "meret.db")
    qtbot.addWidget(window)

    window.open_folder("/travel")
    qtbot.waitUntil(lambda: window.grid.count() == 1, timeout=2000)
    window.grid.setCurrentRow(0)
    qtbot.waitUntil(lambda: window.preview.pixmap() is not None
        and not window.preview.pixmap().isNull(), timeout=2000)

def test_thumbnails_are_batched_and_the_queue_refills_on_completion(tmp_path, qapp, qtbot):
    """The bug: a zero-timer refilled the queue at once, so BATCH limited nothing."""
    from meret.core.sqlite_repo import SQLiteRepository
    from meret.gui.app import BATCH, MainWindow

    repo = SQLiteRepository(str(tmp_path / "meret.db"))
    window = MainWindow(repo, tmp_path / "meret.db")
    qtbot.addWidget(window)

    submitted, landed = [], []

    def fake_run(work, on_done=None, on_failed=None, args=(), kwargs=None,
                 generation=None, sticky=False):
        submitted.append(args[0])
        landed.append(lambda: on_done(None))  # None: nothing is painted

    window._tasks.run = fake_run
    # Bump first: the listing the window fired on start is still in flight, and it
    # would otherwise land on the queue below and pump it a second time
    generation = window._tasks.bump()
    window._queue = [
        (
            index,
            f"file{index}.png",
            0.0,
            window._add_file_item(f"file{index}.png", f"file{index}.png", index, 0.0, None),
        )
        for index in range(BATCH * 2)
    ]

    window._pump(generation)
    pump(qapp)  # a zero-timer would have fired here and submitted everything

    assert len(submitted) == BATCH  # one batch, not the whole queue
    assert len(window._queue) == BATCH  # the rest waits for the pool

    index = 0
    while index < len(landed):  # a landing batch is what asks for the next one
        landed[index]()
        index += 1

    assert len(submitted) == BATCH * 2
    assert window._queue == []

@pytest.fixture
def menu_window(tmp_path, qapp, qtbot):
    """A window on an empty repository: the menus and the toolbar, nothing else."""
    from meret.core.sqlite_repo import SQLiteRepository
    from meret.gui.app import MainWindow

    repo = SQLiteRepository(str(tmp_path / "meret.db"))
    main = MainWindow(repo, tmp_path / "meret.db")
    qtbot.addWidget(main)
    return main


def _menu_entries(window) -> list:
    """Every menu entry, without separators."""
    return [
        action
        for top in window.menuBar().actions()
        for action in top.menu().actions()
        if not action.isSeparator()
    ]


def test_the_menu_bar_has_the_usual_menus(menu_window):
    from meret.gui.commands import menu_table

    assert [top.text() for top in menu_window.menuBar().actions()] == [
        title for title, _commands in menu_table()
    ]


def test_the_menu_bar_lists_every_command_once(menu_window):
    from meret.gui.commands import command_table, short_label

    entries = _menu_entries(menu_window)

    assert len(entries) == len(command_table())
    assert {action.iconText() for action in entries} == {
        short_label(command) for command, _ in command_table()
    }


def test_a_menu_entry_is_the_very_same_action_as_the_toolbar(menu_window):
    """A copy would carry a second shortcut and drift away from the button."""
    in_toolbar = {a for a in menu_window.toolbar.actions() if not a.isSeparator()}

    assert set(_menu_entries(menu_window)) == in_toolbar

def _answer(monkeypatch, role):
    """Make the next QMessageBox answer as if the button with `role` was clicked.

    Roles, not labels: the tests must not care whether a catalog is loaded.
    """
    from PySide6.QtWidgets import QMessageBox

    monkeypatch.setattr(QMessageBox, "exec", lambda box: 0)
    monkeypatch.setattr(
        QMessageBox,
        "clickedButton",
        lambda box: next((b for b in box.buttons() if box.buttonRole(b) == role), None),
    )


def test_init_db_creates_the_database_when_it_is_missing(tmp_path, qapp, qtbot, monkeypatch):
    from PySide6.QtWidgets import QMessageBox

    from meret.core.sqlite_repo import SQLiteRepository
    from meret.gui.app import MainWindow

    asked = []
    monkeypatch.setattr(QMessageBox, "exec", lambda box: asked.append(box) or 0)
    # The repository lives in another file, so _db_path is the missing one
    window = MainWindow(SQLiteRepository(str(tmp_path / "other.db")), tmp_path / "meret.db")
    qtbot.addWidget(window)

    window._init_db()

    assert asked == []  # nothing to ask about
    # The file is created on the pool, and the window is pointed at it afterwards
    qtbot.waitUntil(lambda: window._db_path.is_file(), timeout=2000)


def test_init_db_recreates_the_database_here(tmp_path, qapp, qtbot, monkeypatch):
    from PySide6.QtWidgets import QMessageBox

    from meret.core.models import FileEntry, FileType
    from meret.core.sqlite_repo import SQLiteRepository
    from meret.gui.app import MainWindow

    db = tmp_path / "meret.db"
    window = MainWindow(SQLiteRepository(str(db)), db)
    qtbot.addWidget(window)
    file_id = window._repo.save_file(
        FileEntry(filename="a.png", filepath=str(tmp_path / "a.png"), type=FileType.IMAGE)
    )
    window._repo.add_tag(file_id, "travel")
    assert window._repo.get_all_tags()

    _answer(monkeypatch, QMessageBox.ButtonRole.AcceptRole)
    window._init_db()

    assert window._db_path == db
    # The wipe and the reopen run on the pool: wait for the tags to be gone, not
    # for the path, which never changed
    qtbot.waitUntil(lambda: window._repo.get_all_tags() == [], timeout=3000)


def test_init_db_can_put_the_database_in_another_folder(tmp_path, qapp, qtbot, monkeypatch):
    from PySide6.QtWidgets import QFileDialog, QMessageBox

    from meret.core.models import FileEntry, FileType
    from meret.core.sqlite_repo import SQLiteRepository
    from meret.gui.app import MainWindow

    db = tmp_path / "meret.db"
    window = MainWindow(SQLiteRepository(str(db)), db)
    qtbot.addWidget(window)
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()

    _answer(monkeypatch, QMessageBox.ButtonRole.ActionRole)
    monkeypatch.setattr(
        QFileDialog, "getExistingDirectory", staticmethod(lambda *a, **k: str(elsewhere))
    )
    window._init_db()

    qtbot.waitUntil(lambda: window._db_path == elsewhere / "meret.db", timeout=2000)
    assert window._db_path.is_file()
    # and the window works on the new file, not on the old one
    file_id = window._repo.save_file(
        FileEntry(filename="b.png", filepath=str(tmp_path / "b.png"), type=FileType.IMAGE)
    )
    window._repo.add_tag(file_id, "here")
    assert len(window._repo.get_all_tags()) == 1


def test_init_db_cancel_leaves_everything_alone(tmp_path, qapp, qtbot, monkeypatch):
    from PySide6.QtWidgets import QMessageBox

    from meret.core.models import FileEntry, FileType
    from meret.core.sqlite_repo import SQLiteRepository
    from meret.gui.app import MainWindow

    db = tmp_path / "meret.db"
    window = MainWindow(SQLiteRepository(str(db)), db)
    qtbot.addWidget(window)
    file_id = window._repo.save_file(
        FileEntry(filename="a.png", filepath=str(tmp_path / "a.png"), type=FileType.IMAGE)
    )
    window._repo.add_tag(file_id, "travel")

    _answer(monkeypatch, QMessageBox.ButtonRole.RejectRole)
    window._init_db()

    assert window._db_path == db
    assert len(window._repo.get_all_tags()) == 1  # untouched