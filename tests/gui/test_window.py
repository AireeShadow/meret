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