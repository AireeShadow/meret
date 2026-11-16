"""The window's state and its lifetime: what the mixins are given."""

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from PySide6.QtCore import Qt, QTimer
from PySide6.QtGui import QAction
from PySide6.QtWidgets import QApplication, QMainWindow, QWidget

from meret.core.config import get_config
from meret.core.locale import _
from meret.gui.theme import apply_theme
from meret.gui.thumbnails import ThumbnailCache
from meret.gui.workers import TaskRunner
from meret.vfs.tree import VirtualTree

if TYPE_CHECKING:
    from PySide6.QtWidgets import (
        QComboBox,
        QLabel,
        QListWidget,
        QListWidgetItem,
        QTabWidget,
        QToolBar,
    )

    from meret.gui.fs_panel import RealFsPanel
    from meret.gui.tag_tree import TagTree
    from meret.gui.viewer import ViewerView

logger = logging.getLogger(__name__)


class WindowBase(QMainWindow):
    """State, the first paint and the shutdown.

    Everything the mixins read is created here and nowhere else: one place to look
    when an attribute is None at the wrong moment.

    The TYPE_CHECKING block below is the contract in both directions. The widgets are
    built by LayoutMixin and the methods are spread across six mixins, so without the
    declarations every `self._say(...)` in a sibling reads as "no attribute _say" -
    true of that class, false of the object it is mixed into. One place to look, one
    place to extend: adding a cross-mixin member means adding one line here.
    """

    ROLE_VPATH = Qt.ItemDataRole.UserRole
    ROLE_SOURCE = Qt.ItemDataRole.UserRole + 1
    ROLE_MTIME = Qt.ItemDataRole.UserRole + 2
    ROLE_FILE_ID = Qt.ItemDataRole.UserRole + 3

    #: A message that must outlive the next refresh. Set through _say(), read by
    #: _update_status(), cleared when the user asks for something new.
    _notice: str | None = None

    if TYPE_CHECKING:
        # Widgets: built by LayoutMixin
        tags: TagTree
        fs: RealFsPanel
        grid: QListWidget
        preview: QLabel
        tabs: QTabWidget
        viewer: ViewerView
        drives: QComboBox
        toolbar: QToolBar
        _status_label: QLabel
        _poll: QTimer

        # LayoutMixin
        def _build_ui(self) -> None: ...
        def _build_actions(self) -> None: ...
        def _build_toolbar(self) -> None: ...
        def _build_menus(self) -> None: ...

        # PollMixin
        def _start_poll(self) -> None: ...

        # ContentsMixin
        def open_folder(self, path: str) -> None: ...
        def _refresh(self, panel: bool = False) -> None: ...
        def _say(self, text: str) -> None: ...
        def _update_status(self) -> None: ...
        def _pump(self, generation: int) -> None: ...
        def _show_preview(self) -> None: ...
        def _open_in_viewer(self, item: QListWidgetItem) -> None: ...
        def _next_file(self) -> None: ...
        def _previous_file(self) -> None: ...
        def _on_tab_changed(self, index: int) -> None: ...
        def _on_real_activated(self, path: str) -> None: ...
        def _run_search(self, query: str) -> None: ...

        # CommandsMixin
        def run_command(self, cmd: str) -> None: ...

        # DatabaseMixin
        def _init_db(self) -> None: ...

        # MountMixin
        def _mount(self) -> None: ...
        def _umount(self) -> None: ...

    def __init__(self, repo, db_path, parent: QWidget | None = None) -> None:
        super().__init__(parent)

        config = get_config().config.gui

        # State first: the first paint happens on the last line of __init__. Every
        # attribute is annotated, because a bare assignment lets mypy infer the type of
        # the first value it sees - and `self._version = None` then makes `= version`
        # an error in every mixin that reads it.
        self._repo = repo
        self._db_path: Path = Path(db_path)
        # None only for the moment set_tree() is given nothing, see TagTree.set_tree
        self._tree: VirtualTree | None = VirtualTree(repo)
        # The scope keeps two databases from sharing thumbnail keys: a file id is
        # only unique inside one of them
        self._thumbs: ThumbnailCache = ThumbnailCache(
            size=config.thumbnail_size, scope=str(self._db_path)
        )
        self._tasks: TaskRunner = TaskRunner(self)
        self._queue: list[tuple] = []
        self._selection: set[str] = set()
        self._vpath: str = "/"
        self._search: str | None = None
        self._version: int | None = None
        self._preview_token: int = 0
        self._sort_index: int = 0
        self._actions: dict[str, QAction] = {}
        # True while a modal dialog is up: the grid underneath is not looked at, so
        # nothing has to be decoded and painted into it
        self._paused: bool = False

        # The palette before the icons: they are tinted with its WindowText colour,
        # so a dark theme must be in place or every icon renders black
        apply_theme(QApplication.instance(), config.theme)
        # The theme in effect, so a settings save can tell a change from a no-op:
        # re-polishing the app is a visible stall while the grid is full
        self._theme: str = config.theme

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
