"""The help dialog is built from the palette table, so it must list all of it."""

import pytest

pytest.importorskip("PySide6")


@pytest.fixture
def captured(monkeypatch):
    """Catch the text instead of opening a modal dialog."""
    from PySide6.QtWidgets import QMessageBox

    box = {}
    monkeypatch.setattr(
        QMessageBox,
        "information",
        staticmethod(
            lambda parent, title, text, *a, **k: box.update(title=title, text=text)
        ),
    )
    return box


@pytest.fixture
def window(tmp_path, qapp, qtbot):
    from meret.core.sqlite_repo import SQLiteRepository
    from meret.gui.app import MainWindow

    repo = SQLiteRepository(str(tmp_path / "meret.db"))
    main = MainWindow(repo, tmp_path / "meret.db")
    qtbot.addWidget(main)
    return main


def _rows(text):
    return [line.strip() for line in text.splitlines() if line.startswith("    ")]


def test_help_lists_every_command_once(window, captured):
    from meret.gui.commands import command_table

    window._help()

    rows = _rows(captured["text"])
    assert len(rows) == len(command_table())
    assert sorted(row.split(None, 1)[1] for row in rows) == sorted(
        description for _command, description in command_table()
    )


def test_help_groups_navigation_before_commands(window, captured):
    from meret.core.locale import _

    window._help()

    text = captured["text"]
    assert f"{_('Navigation')}:" in text
    assert f"{_('Commands')}:" in text
    assert text.index(_("Navigation")) < text.index(_("Commands"))


def test_no_two_commands_share_a_key():
    """Two commands on one key means one of them is unreachable."""
    from meret.gui.commands import SHORTCUTS

    keys = [key for key in SHORTCUTS.values() if key]
    assert len(set(keys)) == len(keys)

def test_help_columns_are_separated(window, captured):
    """A key wider than the column used to run straight into the description."""
    window._help()

    for row in _rows(captured["text"]):
        key, separator, description = row.partition(" ")
        assert separator, row
        assert description.startswith(" "), f"key glued to description: {row!r}"


def test_help_keeps_every_key_whole(window, captured):
    """The key must survive as one token: 'Ctrl+Shift+TRemove...' loses it."""
    from meret.gui.commands import SHORTCUTS

    window._help()

    keys = [row.split()[0] for row in _rows(captured["text"])]
    assert set(keys) <= {key for key in SHORTCUTS.values() if key} | {"—"}