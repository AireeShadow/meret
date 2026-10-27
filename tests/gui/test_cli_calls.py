"""What the GUI hands to the CLI. `umount` is the trap: it declares no --db."""

from types import SimpleNamespace

import pytest

pytest.importorskip("PySide6")


class _FakeSubprocess:
    """Stands in for the subprocess module inside meret.gui.app only."""

    def __init__(self, fail: bool = False) -> None:
        self.spawned = []
        self.ran = []
        self.fail = fail

    def Popen(self, command, **kwargs):
        if self.fail:
            raise OSError("no meret on PATH")
        self.spawned.append(command)
        return SimpleNamespace(pid=1)

    def run(self, command, **kwargs):
        if self.fail:
            raise OSError("no meret on PATH")
        self.ran.append(command)
        return SimpleNamespace(returncode=0, stderr="")


@pytest.fixture
def window(tmp_path, qapp, qtbot):
    from meret.core.sqlite_repo import SQLiteRepository
    from meret.gui.app import MainWindow

    repo = SQLiteRepository(str(tmp_path / "meret.db"))
    main = MainWindow(repo, tmp_path / "meret.db")
    qtbot.addWidget(main)
    return main


@pytest.fixture
def fake(monkeypatch):
    import meret.gui.app as app_module

    stub = _FakeSubprocess()
    monkeypatch.setattr(app_module, "subprocess", stub)
    return stub


def _answer(monkeypatch, text="M:"):
    from PySide6.QtWidgets import QInputDialog

    monkeypatch.setattr(
        QInputDialog, "getText", staticmethod(lambda *a, **k: (text, bool(text)))
    )


def test_the_cli_builder_puts_the_flag_where_the_parser_expects_it(window):
    assert window._cli("mount", "M:") == [
        "meret",
        "mount",
        "M:",
        "--db",
        str(window._db_path),
    ]
    assert window._cli("umount", "M:", with_db=False) == ["meret", "umount", "M:"]


def test_mount_spawns_the_cli_with_the_database(window, fake, monkeypatch):
    _answer(monkeypatch)

    window._mount()

    assert fake.spawned == [["meret", "mount", "M:", "--db", str(window._db_path)]]
    assert fake.ran == []  # a mount blocks, so it must not be waited for


def test_umount_runs_without_the_database_flag(window, fake, monkeypatch):
    """`umount_cmd` takes only the mount point: a stray --db is a typer error."""
    _answer(monkeypatch)

    window._umount()

    assert fake.ran == [["meret", "umount", "M:"]]
    assert fake.spawned == []


def test_a_cancelled_dialog_runs_nothing(window, fake, monkeypatch):
    _answer(monkeypatch, text="")

    window._mount()
    window._umount()

    assert fake.spawned == [] and fake.ran == []


def test_a_missing_cli_is_reported_not_raised(window, monkeypatch):
    from PySide6.QtWidgets import QInputDialog

    import meret.gui.app as app_module

    monkeypatch.setattr(app_module, "subprocess", _FakeSubprocess(fail=True))
    monkeypatch.setattr(QInputDialog, "getText", staticmethod(lambda *a, **k: ("M:", True)))

    window._mount()

    assert "no meret on PATH" in window._status_label.text()