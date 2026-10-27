"""Shared helpers and fixtures for the GUI test suite.

Qt has to run headless, so QT_QPA_PLATFORM is set before the first Qt import.
Everything test modules import directly (pump) is defined unconditionally at
module level - never inside a fixture and never after a call that can raise.
"""

import os
import time
from collections.abc import Callable, Iterator

import pytest

# Must be set before Qt is imported for the first time
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

try:  # PySide6 is an optional extra, so never importorskip() at module level
    from PySide6.QtWidgets import QApplication
except ImportError:  # pragma: no cover - exercised without the gui extra
    QApplication = None  # type: ignore[assignment]

try:
    import pytestqt  # noqa: F401

    HAS_PYTEST_QT = True
except ImportError:  # pragma: no cover - pytest-qt is an optional test dep
    HAS_PYTEST_QT = False

HAS_QT = QApplication is not None

if not HAS_QT:  # pragma: no cover - keep the suite green without PySide6
    collect_ignore_glob = ["*.py"]


def pump(target: object | None = None, ms: int = 50) -> None:
    """Process pending Qt events for roughly ms milliseconds.

    Accepts both call styles used across the suite: pump(qapp) and pump(50).

    A nested event loop with a quit timer, not processEvents() in a loop.
    processEvents() returns only once the queue is empty, and it keeps processing
    whatever the handlers queue while it runs - so one widget that keeps posting to
    itself makes a single call never return, the deadline is never reached, and the
    suite looks hung. A nested loop checks its exit flag between dispatches, so the
    timer always gets its turn and pump() always ends.
    """
    if isinstance(target, (int, float)):
        ms, target = int(target), None
    app = target if target is not None else (QApplication.instance() if HAS_QT else None)
    if app is None:
        return

    from PySide6.QtCore import QEventLoop, QTimer

    loop = QEventLoop()
    QTimer.singleShot(max(1, ms), loop.quit)
    loop.exec()


def pump_until(predicate: Callable[[], bool], timeout_ms: int = 2000) -> bool:
    """Pump events until predicate returns True or the timeout expires."""
    deadline = time.monotonic() + timeout_ms / 1000.0
    while time.monotonic() < deadline:
        if predicate():
            return True
        pump(10)
    return predicate()

if HAS_QT:  # the tally needs a real Qt; the rest of the file imports without it

    import threading

    from PySide6.QtCore import QObject

    class EventTally(QObject):
        """Counts every event the application delivers, by receiver and event type.

        notify() does the same job in the app, but the suite runs on pytest-qt's own
        QApplication: an application-wide event filter works with whatever is there.
        A flood is what makes processEvents() never settle, so a test can assert on
        it - and the report names the widget instead of leaving it to a guess.

        The window report is printed by a daemon thread: a hung test never reaches
        its own assertions, so the flood has to be readable while it happens.
        """

        PERIOD = 2.0

        def __init__(self, app: object) -> None:
            super().__init__()
            self._app = app
            self._counts: dict[tuple, int] = {}
            self._started = time.monotonic()
            app.installEventFilter(self)

        def eventFilter(self, receiver, event) -> bool:
            """Count every event and let it through untouched."""
            pair = (type(receiver).__name__, type(event).__name__)
            self._counts[pair] = self._counts.get(pair, 0) + 1
            return False

        def watch(self) -> None:
            """Print a window every PERIOD seconds, from a daemon thread."""
            while True:
                time.sleep(self.PERIOD)
                self._emit(self.take())

        @staticmethod
        def _emit(text: str) -> None:
            """Write straight to file descriptor 2.

            Never through sys.stderr: pytest swaps it for every test and Textual
            wraps it for the TUI ones, and the stream underneath is closed by the
            time this thread wakes up - which raised "invalid handle" on Windows.
            A closed descriptor is the only remaining failure, and that is silent.
            """
            try:
                os.write(2, (text + "\n").encode("utf-8", "replace"))
            except OSError:
                pass

        def take(self, top: int = 6) -> str:
            """The window that just passed, then a fresh one."""
            busiest = sorted(self._counts.items(), key=lambda item: item[1], reverse=True)
            spent = max(time.monotonic() - self._started, 1e-6)
            lines = [f"[tally] {sum(self._counts.values()) / spent:.0f} events/s"]
            lines += [
                f"[tally]     {count:>8}  {name} in {receiver}"
                for (receiver, name), count in busiest[:top]
            ]
            self.reset()
            return "\n".join(lines)

        def reset(self) -> None:
            """Start a fresh window: the rate is measured from here."""
            self._counts = {}
            self._started = time.monotonic()

        def rate(self) -> float:
            """Events per second since the last reset."""
            spent = max(time.monotonic() - self._started, 1e-6)
            return sum(self._counts.values()) / spent

        def report(self, top: int = 8) -> str:
            """The rate and the busiest receivers, ready to print."""
            busiest = sorted(self._counts.items(), key=lambda item: item[1], reverse=True)
            lines = [f"[tally] {self.rate():.0f} events/s"]
            lines += [
                f"[tally]     {count:>8}  {name} in {receiver}"
                for (receiver, name), count in busiest[:top]
            ]
            return "\n".join(lines)

        def stop(self) -> None:
            """Detach from the application."""
            self._app.removeEventFilter(self)

    @pytest.fixture
    def tally(qapp: object) -> Iterator["EventTally"]:
        """Counts every event while a test runs. For tests about event floods."""
        counter = EventTally(qapp)
        yield counter
        counter.stop()

    @pytest.fixture(scope="session", autouse=True)
    def event_watchdog(qapp: object) -> Iterator[None]:
        """With MERET_WATCHDOG=1, print what the event loop is chewing on.

        From a thread, because a hung test never gets to its own report: the flood
        has to be readable while it is happening.
        """
        if not os.environ.get("MERET_WATCHDOG"):
            yield None
            return
        counter = EventTally(qapp)
        threading.Thread(target=counter.watch, daemon=True).start()
        yield None
        counter.stop()

@pytest.fixture(autouse=True)
def quiet_dialogs(monkeypatch) -> None:
    """No modal dialog may block a test: each one runs its own event loop.

    Nothing clicks in a headless run, so a command that opens a dialog would wait
    forever. Every answer here is a sensible default, and a test that wants a
    specific one patches the same callable itself - its patch is applied later and
    wins.
    """
    from PySide6.QtWidgets import QDialog, QFileDialog, QInputDialog, QMessageBox

    from meret.gui.commands import CommandPalette
    from meret.gui.settings import SettingsDialog

    monkeypatch.setattr(QMessageBox, "exec", lambda box: 0)
    monkeypatch.setattr(QMessageBox, "clickedButton", lambda box: box.defaultButton())
    monkeypatch.setattr(
        QMessageBox,
        "information",
        staticmethod(lambda *a, **k: QMessageBox.StandardButton.Ok),
    )
    monkeypatch.setattr(
        QMessageBox,
        "question",
        staticmethod(lambda *a, **k: QMessageBox.StandardButton.Yes),
    )
    monkeypatch.setattr(QInputDialog, "getText", staticmethod(lambda *a, **k: ("", False)))
    monkeypatch.setattr(QInputDialog, "getItem", staticmethod(lambda *a, **k: ("", False)))
    monkeypatch.setattr(QFileDialog, "getExistingDirectory", staticmethod(lambda *a, **k: ""))
    monkeypatch.setattr(QFileDialog, "getSaveFileName", staticmethod(lambda *a, **k: ("", "")))
    monkeypatch.setattr(CommandPalette, "exec", lambda self: QDialog.DialogCode.Rejected)
    monkeypatch.setattr(
        SettingsDialog,
        "open",
        lambda self: self.finished.emit(QDialog.DialogCode.Rejected),
    )

if not HAS_PYTEST_QT:  # pragma: no cover - only when pytest-qt is absent

    @pytest.fixture(scope="session")
    def qapp() -> Iterator[object]:
        """Session-wide QApplication, mirroring pytest-qt's own fixture."""
        if not HAS_QT:
            pytest.skip("PySide6 is not installed")
        app = QApplication.instance() or QApplication([])
        yield app
        pump(20)


@pytest.fixture(scope="session")
def gui_app(qapp: object) -> object:
    """Alias so both fixture names work regardless of pytest-qt presence."""
    return qapp