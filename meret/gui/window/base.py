"""The window's state and its lifetime: what the mixins are given."""

import logging
from pathlib import Path

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QMainWindow, QWidget
from PySide6.QtGui import QAction

from meret.core.config import get_config
from meret.core.locale import _
from meret.gui.theme import apply_theme
from meret.gui.thumbnails import ThumbnailCache
from meret.gui.workers import TaskRunner
from meret.vfs.tree import VirtualTree

logger = logging.getLogger(__name__)


class WindowBase(QMainWindow):
    """State, the first paint and the shutdown.

    Everything the mixins read is created here and nowhere else: one place to look
    when an attribute is None at the wrong moment.
    """

    ROLE_VPATH = Qt.ItemDataRole.UserRole
    ROLE_SOURCE = Qt.ItemDataRole.UserRole + 1
    ROLE_MTIME = Qt.ItemDataRole.UserRole + 2
    ROLE_FILE_ID = Qt.ItemDataRole.UserRole + 3

    #: A message that must outlive the next refresh. Set through _say(), read by
    #: _update_status(), cleared when the user asks for something new.
    _notice: str | None = None

    def __init__(self, repo, db_path, parent: QWidget | None = None) -> None:
        super().__init__(parent)

        config = get_config().config.gui

        # State first: the first paint happens on the last line of __init__
        self._repo = repo
        self._db_path = Path(db_path)
        # None only for the moment set_tree() is given nothing, see TagTree.set_tree
        self._tree: VirtualTree | None = VirtualTree(repo)
        # The scope keeps two databases from sharing thumbnail keys: a file id is
        # only unique inside one of them
        self._thumbs = ThumbnailCache(size=config.thumbnail_size, scope=str(self._db_path))
        self._tasks = TaskRunner(self)
        self._queue: list[tuple] = []
        self._selection: set[str] = set()
        self._vpath = "/"
        self._search: str | None = None
        self._version: int | None = None
        self._preview_token = 0
        self._sort_index = 0
        self._actions: dict[str, QAction] = {}
        # True while a modal dialog is up: the grid underneath is not looked at, so
        # nothing has to be decoded and painted into it
        self._paused = False

        # The palette before the icons: they are tinted with its WindowText colour,
        # so a dark theme must be in place or every icon renders black
        apply_theme(QApplication.instance(), config.theme)
        # The theme in effect, so a settings save can tell a change from a no-op:
        # re-polishing the app is a visible stall while the grid is full
        self._theme = config.theme

        # Widgets next
        self._build_ui()
        self._build_actions()
        self._build_toolbar()
        self._build_menus()

        self.setWindowTitle(_("meret"))
        self.resize(config.window_width, config.window_height)

        # Outside changes: the tree is invalidated, never re-read per stat
        self._start_poll()

        # The tree has to be filled once by hand: reload() is otherwise only
        # reached through the outside-change path, which leaves it empty on start
        self.tags.reload()

        # First paint - nothing may come after this
        self.open_folder("/")

    def closeEvent(self, event) -> None:
        self._poll.stop()
        self._tasks.shutdown()
        super().closeEvent(event)
