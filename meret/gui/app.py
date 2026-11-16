"""The Qt front-end: the window and its entry point.

Qt objects live on the GUI thread only. Anything that touches SQLite or decodes an
image goes through TaskRunner and comes back as a signal. The tag panel reads the same
VirtualTree a mount would serve, so the two can never disagree, and the real filesystem
panel is plain Qt - it only picks a path to scan or to tag.

Mounting and unmounting shell out to `meret mount` / `meret umount` instead of
importing a backend, so opening a window never drags FUSE or WinFsp in.

What is left in this module is the assembly: the window's state and lifetime
(WindowBase), its shape (LayoutMixin), its browser (ContentsMixin), its commands
(CommandsMixin), the database switch (DatabaseMixin), the shell-outs (MountMixin) and
the poll (PollMixin) are one mixin each. A mixin reads and writes the attributes
WindowBase sets up, which is why the split needs no wiring.
"""

import faulthandler
import sys
from pathlib import Path

from PySide6.QtWidgets import QApplication, QWidget

from meret.core.debug import enabled
from meret.core.sqlite_repo import SQLiteRepository
from meret.gui.diagnostics import TracedApplication, quiet_media_logs
from meret.gui.window.base import WindowBase
from meret.gui.window.commands import CommandsMixin
from meret.gui.window.contents import ContentsMixin
from meret.gui.window.databases import DatabaseMixin
from meret.gui.window.layout import LayoutMixin
from meret.gui.window.mounts import MountMixin
from meret.gui.window.polling import PollMixin


class MainWindow(
    LayoutMixin,
    ContentsMixin,
    CommandsMixin,
    DatabaseMixin,
    MountMixin,
    PollMixin,
    WindowBase,
):
    """Browse the tag tree, the files under one tag, and a preview.

    WindowBase is last, and that is load-bearing twice over.

    For mypy: every mixin inherits WindowBase under TYPE_CHECKING, so listing it first
    would put a parent before its own subclasses and the MRO cannot be linearised.

    At runtime: the mixins are plain `object` subclasses, so the chain walks past all
    of them and lands on WindowBase.__init__ - which is the only real constructor here.
    Reversed, `super().__init__(repo, db_path, parent)` would reach `object.__init__`
    with three arguments.

    The explicit __init__ is not decoration either: without it the call is forwarded by
    whatever happens to be first in the MRO, and a plain-class mixin turns
    `MainWindow(repo, db_path)` into `WindowBase.__init__()` with the arguments eaten.

    It must not do any of the building itself. WindowBase.__init__ builds the widgets,
    and a second copy here does not replace them: the menu bar is one object per window,
    so `_build_menus` runs twice on it and every menu appears twice.
    """

    def __init__(self, repo, db_path, parent: QWidget | None = None) -> None:
        super().__init__(repo, db_path, parent)


def run_gui(db_path: Path, argv: list[str] | None = None) -> int:
    """Open the database, show the window, run the event loop. Returns the exit code.

    With --debug watchdog the run is traced: every thread's Python stack is dumped
    every three seconds, and the application counts and times every event. A freeze
    the suite cannot reproduce leaves no other trace, and "the loop stopped" and
    "the loop is spinning" need opposite fixes - the trace tells them apart.
    """
    quiet_media_logs()
    watch = enabled("watchdog")
    if watch:
        faulthandler.dump_traceback_later(3.0, repeat=True)

    repo = SQLiteRepository(str(db_path))
    app = QApplication.instance()
    if app is None:
        app = (
            TracedApplication(argv or [sys.argv[0]])
            if watch
            else QApplication(argv or [sys.argv[0]])
        )
    app.setApplicationName("meret")

    window = MainWindow(repo, db_path)
    window.show()
    try:
        return app.exec()
    finally:
        faulthandler.cancel_dump_traceback_later()
        window.close()
