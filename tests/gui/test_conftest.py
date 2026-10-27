"""The harness helpers themselves: they must survive a swapping stderr."""

import sys

import pytest

pytest.importorskip("PySide6")


def test_the_watchdog_prints_without_touching_sys_stderr(monkeypatch):
    """pytest swaps sys.stderr for every test and Textual wraps it for the TUI ones.

    The stream behind it is closed by the time the watchdog thread writes, which
    raised "invalid handle" - so the watchdog writes to the descriptor instead.
    """
    from tests.gui.conftest import EventTally

    class Broken:
        """A stream that fails the way a closed handle does."""

        def write(self, _text: str) -> None:
            raise OSError(6, "invalid handle")

        def flush(self) -> None:
            raise OSError(6, "invalid handle")

    monkeypatch.setattr(sys, "stderr", Broken())

    EventTally._emit("marker")  # must not raise