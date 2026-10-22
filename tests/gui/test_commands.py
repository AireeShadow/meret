"""The palette covers the TUI one to one. Icons say what a row is, tooltips why."""

import pytest

from tests.gui.conftest import pump

pytest.importorskip("PySide6")

#: The one command the TUI palette spells differently from the action it calls.
PALETTE_ALIASES = {"init": "init_db"}


def test_command_table_covers_the_whole_tui():
    """Every TUI binding, every palette entry, plus untag - and nothing else."""
    from meret.cli.tui import CommandPalette as TuiPalette
    from meret.cli.tui import MeretApp
    from meret.gui.commands import command_table

    expected = {binding.action for binding in MeretApp.BINDINGS}
    expected |= {PALETTE_ALIASES.get(name, name) for name, _ in TuiPalette().commands}
    expected |= {"untag"}  # in the TUI palette, wired up only in the GUI

    assert {command for command, _ in command_table()} == expected


def test_shared_descriptions_are_the_tui_ones():
    """Where the TUI names a command, the GUI uses that exact wording."""
    from meret.cli.tui import CommandPalette as TuiPalette
    from meret.gui.commands import command_table

    ours = dict(command_table())
    for command, description in TuiPalette().commands:
        assert ours[PALETTE_ALIASES.get(command, command)] == description


def test_every_command_has_a_key_and_a_label():
    from meret.gui.commands import SHORTCUTS, command_table, short_label

    for command, _description in command_table():
        assert command in SHORTCUTS
        assert short_label(command)


def test_matches_orders_exact_then_prefix_then_anywhere():
    from meret.gui.commands import command_table, matches

    assert matches("") == [command for command, _ in command_table()]
    assert matches("scan") == ["scan"]
    assert matches("sca")[0] == "scan"
    assert matches("un")[0] == "untag"


def test_every_command_has_a_key_and_a_label():
    """An empty key is allowed nowhere: the TUI binds every action it offers."""
    from meret.gui.commands import SHORTCUTS, command_table, short_label

    for command, _description in command_table():
        assert SHORTCUTS[command], command
        assert short_label(command)


def test_no_command_falls_through_to_the_not_available_note(
    tmp_path, qapp, qtbot, monkeypatch
):
    """Each entry reaches a real handler. Dialogs are stubbed: a nested loop in a
    test nobody closes would just hang."""
    from PySide6.QtGui import QDesktopServices
    from PySide6.QtWidgets import QDialog, QInputDialog, QMessageBox

    from meret.core.sqlite_repo import SQLiteRepository
    from meret.gui.app import MainWindow
    from meret.gui.commands import CommandPalette, command_table

    monkeypatch.setattr(QInputDialog, "getText", staticmethod(lambda *a, **k: ("", False)))
    monkeypatch.setattr(QInputDialog, "getItem", staticmethod(lambda *a, **k: ("", False)))
    monkeypatch.setattr(QMessageBox, "information", staticmethod(lambda *a, **k: None))
    monkeypatch.setattr(QDesktopServices, "openUrl", staticmethod(lambda *a, **k: True))
    monkeypatch.setattr(CommandPalette, "exec", lambda self: QDialog.DialogCode.Rejected)

    repo = SQLiteRepository(str(tmp_path / "meret.db"))
    window = MainWindow(repo, tmp_path / "meret.db")
    qtbot.addWidget(window)

    for command, _description in command_table():
        window.run_command(command)
        assert "isn't available" not in window._status_label.text(), command

def test_missing_preview_keeps_an_icon_and_explains_itself(tmp_path, qapp, qtbot):
    """A file whose source is gone: still an icon, and the tooltip says why."""
    from meret.core.models import FileEntry, FileType
    from meret.core.sqlite_repo import SQLiteRepository
    from meret.gui.app import MainWindow

    repo = SQLiteRepository(str(tmp_path / "meret.db"))
    gone = tmp_path / "gone.png"
    file_id = repo.save_file(
        FileEntry(filename=gone.name, filepath=str(gone), type=FileType.IMAGE)
    )
    repo.add_tag(file_id, "travel")

    window = MainWindow(repo, tmp_path / "meret.db")
    qtbot.addWidget(window)
    window.open_folder("/travel")
    qtbot.waitUntil(lambda: window.grid.count() == 1, timeout=2000)

    item = window.grid.item(0)
    assert not item.icon().isNull()  # the placeholder, not an empty row
    qtbot.waitUntil(lambda: len(item.toolTip().splitlines()) == 3, timeout=2000)
    assert "ffmpeg" not in item.toolTip()  # an image, not a video

def test_sort_cycles_columns(tmp_path, qapp, qtbot):
    """Name, then size, then date - the TUI's order, on the view's own columns."""
    from PySide6.QtCore import Qt

    from meret.core.sqlite_repo import SQLiteRepository
    from meret.gui.app import MainWindow

    repo = SQLiteRepository(str(tmp_path / "meret.db"))
    window = MainWindow(repo, tmp_path / "meret.db")
    qtbot.addWidget(window)

    calls = []
    window.fs.sortByColumn = lambda column, order: calls.append((column, order))

    for _ in range(3):
        window._sort()

    assert calls == [
        (1, Qt.SortOrder.DescendingOrder),  # size, largest first
        (3, Qt.SortOrder.DescendingOrder),  # date, newest first
        (0, Qt.SortOrder.AscendingOrder),   # name, A to Z
    ]


def test_sort_uses_the_views_own_slot(tmp_path, qapp, qtbot):
    """Patching fs.sortByColumn would hide a missing slot, so do not patch it here.

    QTreeView has sortByColumn; `sort` belongs to QTreeWidget and never existed on
    the view - which is exactly how the AttributeError got in.
    """
    from PySide6.QtCore import Qt

    from meret.core.sqlite_repo import SQLiteRepository
    from meret.gui.app import MainWindow

    repo = SQLiteRepository(str(tmp_path / "meret.db"))
    window = MainWindow(repo, tmp_path / "meret.db")
    qtbot.addWidget(window)

    landed = []
    window.fs._model.sort = lambda column, order: landed.append((column, order))

    window._sort()  # must not raise

    # With sorting enabled, Qt sorts through the header signal *and* directly once
    # the indicator matches, so the model sees the same call twice. Assert the set,
    # not the count: a future Qt may collapse the two.
    assert landed
    assert set(landed) == {(1, Qt.SortOrder.DescendingOrder)}
    assert window.fs.header().sortIndicatorSection() == 1

def test_palette_shows_every_command_without_scrolling(qapp):
    """A palette you must scroll looks like a palette with missing commands."""
    from meret.gui.commands import CommandPalette, command_table

    palette = CommandPalette()
    palette.show()
    qapp.processEvents()

    assert palette._list.count() == len(command_table())
    row = palette._list.sizeHintForRow(0) or 22
    assert palette._list.viewport().height() >= row * palette._list.count()

def test_palette_rows_separate_key_name_and_description(qapp):
    """Same trap as the help: 'command_palette' is exactly as wide as the column."""
    from PySide6.QtCore import Qt

    from meret.gui.commands import CommandPalette, SHORTCUTS, command_table

    palette = CommandPalette()
    descriptions = dict(command_table())

    for row in range(palette._list.count()):
        item = palette._list.item(row)
        command = item.data(Qt.ItemDataRole.UserRole)
        key = SHORTCUTS.get(command) or "—"

        assert item.text().startswith(f"{key} "), item.text()
        assert f" {descriptions[command]}" in item.text(), item.text()