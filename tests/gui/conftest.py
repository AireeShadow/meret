"""Shared helpers and fixtures for the GUI test suite.

Qt has to run headless, so QT_QPA_PLATFORM is set before the first Qt import.
Everything test modules import directly (pump) is defined unconditionally at
module level - never inside a fixture and never after a call that can raise.
"""
from __future__ import annotations

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
    """
    if isinstance(target, (int, float)):
        ms, target = int(target), None
    app = target if target is not None else (QApplication.instance() if HAS_QT else None)
    if app is None:
        return
    deadline = time.monotonic() + ms / 1000.0
    while time.monotonic() < deadline:
        app.processEvents()
        time.sleep(0.001)


def pump_until(predicate: Callable[[], bool], timeout_ms: int = 2000) -> bool:
    """Pump events until predicate returns True or the timeout expires."""
    deadline = time.monotonic() + timeout_ms / 1000.0
    while time.monotonic() < deadline:
        if predicate():
            return True
        pump(10)
    return predicate()


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