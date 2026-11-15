"""What the GUI hands to the CLI. `umount` is the trap: it declares no --db."""

import pytest

pytest.importorskip("PySide6")


class _Done:
    """What subprocess.run() hands back, as far as the window reads it.

    Three attributes, because _cli_call reads exactly three: the exit code and the
    two streams it turns into a message.
    """

    def __init__(self, returncode: int, stdout: str = "", stderr: str = "") -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


class _FakeSubprocess:
    """Stands in for the whole subprocess module, recording what the window asked for.

    Two lists, not one: `spawned` is what was started and not waited for (a mount
    lives until it is unmounted), `ran` is what was run to completion. Merging them
    makes "a mount must not block the window" impossible to assert.

    The module is patched, not one function: mounts.py uses Popen, DEVNULL and
    TimeoutExpired, and a stub that only has Popen turns a missing constant into an
    AttributeError in the middle of a command - which reads like a bug in the window.
    """

    #: The real value is -3. Nothing compares it, only passes it through.
    DEVNULL = -3

    class TimeoutExpired(Exception):
        """Stands in for subprocess.TimeoutExpired."""

    def __init__(self, fail: bool = False) -> None:
        self.spawned: list[list[str]] = []
        self.ran: list[list[str]] = []
        self.fail = fail

    def Popen(self, command, **kwargs):
        if self.fail:
            raise OSError("no meret here")
        self.spawned.append(list(command))

    def run(self, command, **kwargs):
        if self.fail:
            raise OSError("no meret here")
        self.ran.append(list(command))
        return _Done(0, "Tagged: 0 files, 0 tags", "")


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
    """A subprocess stub in the module that actually calls it.

    Not meret.gui.app: the CLI calls moved to meret.gui.window.mounts, and a
    monkeypatch has to target the module that looks the name up - patching app
    creates an attribute nothing reads and leaves the real subprocess in place.
    """
    from meret.gui.window import mounts

    stub = _FakeSubprocess()
    monkeypatch.setattr(mounts, "subprocess", stub)
    return stub


def _answer(monkeypatch, text="M:"):
    from PySide6.QtWidgets import QInputDialog

    monkeypatch.setattr(QInputDialog, "getText", staticmethod(lambda *a, **k: (text, bool(text))))


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
    """`umount_cmd` takes only the mount point: a stray --db is a typer error.

    `_run_async` is replaced with a synchronous shim. The real one hands the call to
    the task pool, so `fake.ran` is still empty when the assertion runs - and waiting
    for a thread to prove a command line is a test that fails on a loaded machine.
    """
    _answer(monkeypatch)

    calls = []
    window._run_async = lambda command, on_done: calls.append(command)

    window._umount()

    assert calls == [["meret", "umount", "M:"]]


def test_a_cancelled_dialog_runs_nothing(window, fake, monkeypatch):
    _answer(monkeypatch, text="")

    window._mount()
    window._umount()

    assert fake.spawned == [] and fake.ran == []


def test_a_missing_cli_is_reported_not_raised(window, monkeypatch):
    from PySide6.QtWidgets import QInputDialog

    from meret.gui.window import mounts

    monkeypatch.setattr(mounts, "subprocess", _FakeSubprocess(fail=True))
    monkeypatch.setattr(QInputDialog, "getText", staticmethod(lambda *a, **kw: ("M:", True)))

    window._umount()

    assert "no meret" in window._status_label.text() or window._notice is not None


def test_the_cli_call_reports_a_missing_binary(window, fake):
    code, message = window._cli_call(["meret", "umount", "M:"])
    assert code == 0  # the stub answers, nothing raised
    assert fake.ran == [["meret", "umount", "M:"]]
