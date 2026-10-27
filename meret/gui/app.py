"""The Qt front-end: window, panels and the event loop.

Qt objects live on the GUI thread only. Anything that touches SQLite or decodes
an image goes through TaskRunner and comes back as a signal. The tag panel reads
the same VirtualTree a mount would serve, so the two can never disagree, and the
real filesystem panel is plain Qt - it only picks a path to scan or to tag.

Mounting and unmounting shell out to `meret mount` / `meret umount` instead of
importing a backend, so opening a window never drags FUSE or WinFsp in.
"""

import faulthandler
import gc
import logging
import os
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path

from PySide6.QtCore import QDir, QModelIndex, QSize, Qt, QTimer, QUrl, Signal
from PySide6.QtGui import QAction, QColor, QDesktopServices, QIcon, QPalette, QPixmap
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QDialog,
    QFileDialog,
    QFileSystemModel,
    QGroupBox,
    QInputDialog,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QMessageBox,
    QSplitter,
    QStatusBar,
    QStyle,
    QToolBar,
    QTreeView,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
)

from meret.core import tagpaths
from meret.core.config import get_config
from meret.core.indexer import Indexer
from meret.core.locale import _
from meret.core.models import FileEntry, FileType
from meret.core.sqlite_repo import SQLiteRepository
from meret.gui import icons
from meret.gui.commands import (
    SHORTCUTS,
    CommandPalette,
    command_table,
    menu_table,
    short_label,
)
from meret.gui.settings import SettingsDialog, apply_settings
from meret.gui.thumbnails import Reason, Thumbnail, ThumbnailCache
from meret.gui.workers import TaskRunner
from meret.vfs.tree import Node, VirtualTree

logger = logging.getLogger(__name__)

#: Thumbnails requested per round - a huge folder must not flood the pool queue.
BATCH = 24
#: How often the database's data_version is checked for outside changes.
POLL_MS = 1500
#: Preview size, in pixels.
PREVIEW = 512

#: Toolbar icon size. Small enough that all 21 commands fit without Qt hiding any
#: behind its ">>" overflow button, which reads as missing commands.
ICON = 28

#: Toolbar buttons, in order. None is a separator. This is every command: a panel
#: that hides half of them is not "one to one with the TUI".
TOOLBAR: tuple = (
    "refresh",
    "scan",
    "search",
    None,
    "tag",
    "untag",
    "delete",
    None,
    "sort_files",
    None,
    "init_db",
    "config",
    "stats",
    "tree",
    "prune",
    None,
    "mount",
    "umount",
    None,
    "command_palette",
    "help",
    None,
    "quit",
)

#: The help dialog groups these under Navigation, as the TUI's help does.
NAVIGATION_COMMANDS = ("sort_files",)

_DARK_PALETTE = {
    "Window": "#1e1f22",
    "WindowText": "#e6e6e6",
    "Base": "#25262a",
    "AlternateBase": "#2b2d31",
    "Text": "#e6e6e6",
    "Button": "#2b2d31",
    "ButtonText": "#e6e6e6",
    "BrightText": "#ffffff",
    "Highlight": "#3d6ea5",
    "HighlightedText": "#ffffff",
    "ToolTipBase": "#2b2d31",
    "ToolTipText": "#e6e6e6",
    "PlaceholderText": "#9aa0a6",
    "Link": "#6ea8dc",
}

#: Without these, disabled buttons and dimmed text stay bright in dark mode.
_DISABLED_PALETTE = {
    "WindowText": "#7a7d82",
    "Text": "#7a7d82",
    "ButtonText": "#7a7d82",
    "HighlightedText": "#9aa0a6",
}


def _apply_theme(app: QApplication | None, theme: str) -> None:
    """`system` keeps the platform look; `dark` sets a Fusion palette."""
    if app is None or theme == "system":
        return
    app.setStyle("Fusion")
    if theme != "dark":
        app.setPalette(app.style().standardPalette())
        return
    palette = QPalette()
    for role, colour in _DARK_PALETTE.items():
        palette.setColor(getattr(QPalette.ColorRole, role), QColor(colour))
    for role, colour in _DISABLED_PALETTE.items():
        palette.setColor(
            QPalette.ColorGroup.Disabled,
            getattr(QPalette.ColorRole, role),
            QColor(colour),
        )
    app.setPalette(palette)


def _reason_text(reason: str) -> str:
    """Translated tooltip for a missing preview. Resolved at call time."""
    table = {
        Reason.NO_TOOL: _("No preview: ffmpeg is not installed"),
        Reason.NO_FRAME: _("No preview: no frame could be read"),
        Reason.UNREADABLE: _("No preview: the file could not be read"),
        Reason.NO_DECODER: _("No preview for this file type"),
    }
    return table.get(reason, "")


class TagTree(QTreeWidget):
    """The tag hierarchy as folders, straight out of the virtual tree.

    Children are read only when a row is expanded - and off the GUI thread:
    listdir can rebuild the whole snapshot in the repository, so a synchronous
    call on a library with thousands of tags freezes the window mid-click.

    The tree keeps its own generation, so a listing still on its way when the tree
    is rebuilt never touches a row that is already gone.
    """

    tag_selected = Signal(str)
    FILLED = Qt.ItemDataRole.UserRole + 10
    ROLE_TAG_ID = Qt.ItemDataRole.UserRole + 11
    LOADING = Qt.ItemDataRole.UserRole + 12

    def __init__(
        self, tree: VirtualTree, tasks: TaskRunner, parent: QWidget | None = None
    ) -> None:
        super().__init__(parent)
        self._tree = tree
        self._tasks = tasks
        self._generation = 0
        self.setHeaderHidden(True)
        self.itemExpanded.connect(self._on_expanded)
        self.currentItemChanged.connect(self._on_current)

    def set_tree(self, tree: VirtualTree | None) -> None:
        """Point the panel at another database's tree, or at nothing for a moment.

        None is not a detail: the old tree holds the old repository, and a database
        file cannot be deleted while a connection to it is still open.
        """
        self._tree = tree
        if tree is not None:
            self.reload()

    def reload(self) -> None:
        """Rebuild from the root. Anything in flight for the old rows is dropped."""
        self._generation += 1
        self.clear()
        self._fill(self.invisibleRootItem(), "/", self._generation)

    def _fill(self, item: QTreeWidgetItem, vpath: str, generation: int) -> None:
        """Queue a listing for `item`; the row fills itself when it lands.

        Sticky: the folder generation is bumped on every navigation, and a tag
        listing must not be dropped just because the grid re-read a folder.
        """
        item.setData(0, self.LOADING, True)
        self._tasks.run(
            self._tree.listdir,
            on_done=lambda nodes, item=item, gen=generation: self._filled(
                item, nodes, gen
            ),
            on_failed=lambda error, item=item, gen=generation: self._failed(
                item, error, gen
            ),
            args=(vpath,),
            sticky=True,
        )

    def _filled(self, item: QTreeWidgetItem, nodes: list[Node], generation: int) -> None:
        """Replace the placeholder with the real subfolders."""
        if generation != self._generation:
            return  # the tree was rebuilt; this row is gone
        item.takeChildren()
        for node in nodes:
            if not node.is_dir:
                continue
            child = QTreeWidgetItem([node.name])
            child.setData(0, Qt.ItemDataRole.UserRole, node.vpath)
            child.setData(0, self.ROLE_TAG_ID, node.tag_id)
            item.addChild(child)
            child.addChild(QTreeWidgetItem([""]))
        item.setData(0, self.LOADING, False)
        item.setData(0, self.FILLED, True)

    def _failed(self, item: QTreeWidgetItem, error: Exception, generation: int) -> None:
        """A row that failed must say so, never look empty."""
        if generation != self._generation:
            return
        logger.warning("tag tree: cannot list: %s", error)
        item.takeChildren()
        item.addChild(QTreeWidgetItem([str(error)]))
        item.setData(0, self.LOADING, False)
        item.setData(0, self.FILLED, True)

    def _on_expanded(self, item: QTreeWidgetItem) -> None:
        if item.data(0, self.FILLED) or item.data(0, self.LOADING):
            return
        item.takeChildren()
        item.addChild(QTreeWidgetItem(["…"]))
        self._fill(item, item.data(0, Qt.ItemDataRole.UserRole), self._generation)

    def _on_current(self, current: QTreeWidgetItem | None, _previous) -> None:
        if current is not None:
            self.tag_selected.emit(current.data(0, Qt.ItemDataRole.UserRole) or "/")


class RealFsPanel(QTreeView):
    """The whole real filesystem, from its top: drives on Windows, "/" on Unix.

    Qt's own model, so nothing here needs a database connection or a pool thread.
    The view is rooted at the model's own root - an invalid QModelIndex - which is
    the drive list on Windows and "/" on Unix, so one tree covers every drive
    instead of one folder. Navigation reveals a path inside that tree instead of
    re-rooting it, which is what makes the panel show the filesystem, not a folder.
    """

    path_activated = Signal(str)

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._started = time.monotonic()
        self._model = QFileSystemModel(self)
        self._model.setFilter(
            QDir.Filter.AllDirs | QDir.Filter.Files | QDir.Filter.NoDotAndDotDot
        )
        # The watcher is the freeze: QFileSystemModel watches every directory it has
        # ever listed, and a change anywhere in one of them makes it re-read that
        # directory on the GUI thread with a stat per entry - 6.4 seconds in a single
        # event, measured. MERET_WATCH_FS=1 skips the options and leaves the stock
        # model, i.e. exactly what the panel did before the freeze was found.
        if not os.environ.get("MERET_WATCH_FS"):
            self._model.setOption(QFileSystemModel.Option.DontWatchForChanges, True)
            self._model.setOption(QFileSystemModel.Option.DontResolveSymlinks, True)
            # A directory icon is an SHGetFileInfo call per entry on Windows
            self._model.setOption(
                QFileSystemModel.Option.DontUseCustomDirectoryIcons, True
            )
        self.setModel(self._model)
        self.setHeaderHidden(True)
        # Rows are one line high and never wrap, so the view can skip asking every
        # item for its height - which matters with thousands of rows under a root
        self.setUniformRowHeights(True)
        # Sortable, even though the hidden header cannot be clicked: sortByColumn
        # needs the view in the sorting mode to keep the indicator and the model
        # in step
        self.setSortingEnabled(True)
        for column in (1, 2, 3):  # size, type, date - the name column is enough
            self.hideColumn(column)
        self.doubleClicked.connect(self._on_double)
        # Every listing is resolved on the GUI thread, so the cost of revealing a
        # folder is the size of its ancestors. Naming them in the log beats guessing.
        self._model.directoryLoaded.connect(self._log_loaded)
        self.show_filesystem()
        QTimer.singleShot(500, self._report_top)

    def show_filesystem(self) -> None:
        """Show the top of the filesystem and drop the selection.

        The empty root path is the model's own root - the drive list on Windows, "/"
        elsewhere - and it is also what flips the flag canFetchMore() needs before it
        populates anything at all.

        One call only, deliberately: every setRootPath("") makes the model read the
        drive list again, and each drive costs a volumeName() call on the GUI thread.
        setRootPath() with the path it already has returns early, and the view does
        the first fetch itself - which is enough here.
        """
        self._model.setRootPath("")
        self.setRootIndex(QModelIndex())
        self.setCurrentIndex(QModelIndex())

    def reveal(self, path: Path) -> bool:
        """Show `path`: expand what is needed, select it and scroll it into view.

        Deliberately not index(path) alone: that one fetches with fetch=false and
        hands back an invalid index for a folder that is cached but filtered out - a
        hidden one such as AppData. So index() is tried first and setRootPath() is
        the fallback for a path the model has not listed yet - which is also what
        keeps a second reveal of the same folder free of a whole new listing.

        The model's own root goes back to empty on the way out: the drive list *is*
        the root, so a model left rooted at the revealed folder shows that one drive
        and hides the rest.
        """
        target = str(Path(path))
        started = time.perf_counter()

        index = self._model.index(target)
        rerooted = not index.isValid()
        if rerooted:
            index = self._model.setRootPath(target)  # never listed: this lists it
        if not index.isValid():
            if rerooted:
                self._model.setRootPath("")
            return False

        self._expand_ancestors(index)
        self.setCurrentIndex(index)
        self.scrollTo(index, QAbstractItemView.ScrollHint.PositionAtCenter)
        if rerooted:
            self._model.setRootPath("")

        logger.info(
            "revealed %s in %.0f ms%s",
            target,
            (time.perf_counter() - started) * 1000,
            " (listed)" if rerooted else "",
        )
        return True

    def refresh(self) -> None:
        """Re-read the panel from scratch.

        Nothing else does it now that the watcher is off, and setRootPath() with the
        path it already has returns early - hence the step off the root and back,
        which is a real change and therefore a real re-read.
        """
        self._model.setRootPath(QDir.homePath())
        self._model.setRootPath("")

    def _log_loaded(self, path: str) -> None:
        """Log a directory the gatherer finished, with the time since start.

        A directory that shows up here a minute in is the stall: the GUI thread
        resolves every entry of it with a stat, and the model's destructor waits for
        the listing to end. %TEMP% is an ancestor of every pytest tmp_path.
        """
        try:
            logger.info(
                "filesystem loaded after %.1f s: %s",
                time.monotonic() - self._started,
                path or "<drives>",
            )
        except RuntimeError:
            return

    def _report_top(self) -> None:
        """One line in the log: how many entries the panel is actually showing.

        A singleShot outlives the widget it was started from, so the C++ object
        behind self can already be gone by the time this runs - which is a failure
        in whatever test happens to be pumping events at that moment, not here.
        """
        try:
            logger.info(
                "filesystem panel: %d entries at the top", self._model.rowCount()
            )
        except RuntimeError:
            return

    def _expand_ancestors(self, index: QModelIndex) -> None:
        """Expand every row above `index`, top down, so it is on screen.

        A row that is already open is skipped. Expanding makes the model list a
        directory, and the GUI thread resolves every entry of that listing with a
        stat, so expanding an open row repeats the whole cost for nothing - and
        %TEMP% alone is tens of thousands of entries.
        """
        chain: list[QModelIndex] = []
        parent = index.parent()
        while parent.isValid():
            chain.append(parent)
            parent = parent.parent()
        for parent in reversed(chain):
            if not self.isExpanded(parent):
                self.expand(parent)

    def path_of(self, index: QModelIndex) -> Path | None:
        """The real path behind a row, or None for the invalid top index."""
        if not index.isValid():
            return None
        return Path(self._model.filePath(index))

    def current_path(self) -> Path | None:
        return self.path_of(self.currentIndex())

    def _on_double(self, index: QModelIndex) -> None:
        path = self.path_of(index)
        if path is not None:
            self.path_activated.emit(str(path))


class MainWindow(QMainWindow):
    """Browse the tag tree, the files under one tag, and a preview."""

    ROLE_VPATH = Qt.ItemDataRole.UserRole
    ROLE_SOURCE = Qt.ItemDataRole.UserRole + 1
    ROLE_MTIME = Qt.ItemDataRole.UserRole + 2
    ROLE_FILE_ID = Qt.ItemDataRole.UserRole + 3

    def __init__(self, repo, db_path, parent: QWidget | None = None) -> None:
        super().__init__(parent)

        config = get_config().config.gui

        # State first: the first paint happens on the last line of __init__
        self._repo = repo
        self._db_path = Path(db_path)
        self._tree = VirtualTree(repo)
        # The scope keeps two databases from sharing thumbnail keys: a file id is
        # only unique inside one of them
        self._thumbs = ThumbnailCache(
            size=config.thumbnail_size, scope=str(self._db_path)
        )
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
        _apply_theme(QApplication.instance(), config.theme)
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
        self._poll = QTimer(self)
        self._poll.setInterval(POLL_MS)
        self._poll.timeout.connect(self._check_version)
        self._poll.start()

        # The tree has to be filled once by hand: reload() is otherwise only
        # reached through the outside-change path, which leaves it empty on start
        self.tags.reload()

        # First paint - nothing may come after this
        self.open_folder("/")

    def _build_ui(self) -> None:
        splitter = QSplitter(Qt.Orientation.Horizontal, self)

        self.tags = TagTree(self._tree, self._tasks, self)
        self.tags.setMinimumWidth(180)
        self.tags.tag_selected.connect(self.open_folder)

        self.fs = RealFsPanel(self)
        self.fs.path_activated.connect(self._on_real_activated)
        # reveal, not set_root: the panel is rooted at the filesystem top for good,
        # so the database folder is a row to walk to, not a new root
        self.fs.reveal(self._db_path.parent)

        left = QSplitter(Qt.Orientation.Vertical, self)
        tags_box = QGroupBox(_("Virtual FS (Tags)"), self)
        QVBoxLayout(tags_box).addWidget(self.tags)
        fs_box = QGroupBox(_("Real Filesystem"), self)
        QVBoxLayout(fs_box).addWidget(self.fs)
        left.addWidget(tags_box)
        left.addWidget(fs_box)
        left.setSizes([320, 320])
        splitter.addWidget(left)

        self.preview = QLabel(_("Nothing selected"), self)
        self.preview.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.preview.setWordWrap(True)
        self.preview.setMinimumWidth(320)

        self.grid = QListWidget(self)
        self.grid.setViewMode(QListWidget.ViewMode.IconMode)
        self.grid.setResizeMode(QListWidget.ResizeMode.Adjust)
        self.grid.setMovement(QListWidget.Movement.Static)
        self.grid.setUniformItemSizes(True)
        self.grid.setSpacing(6)
        self.grid.setIconSize(QSize(self._thumbs.size, self._thumbs.size))
        self.grid.itemSelectionChanged.connect(self._show_preview)
        self.grid.itemDoubleClicked.connect(self._open_external)

        splitter.addWidget(self.grid)
        splitter.addWidget(self.preview)
        splitter.setStretchFactor(0, 0)
        splitter.setStretchFactor(1, 3)
        splitter.setStretchFactor(2, 2)
        self.setCentralWidget(splitter)

        self._status_label = QLabel(_("No selection"), self)
        bar = QStatusBar(self)
        bar.addPermanentWidget(self._status_label)
        self.setStatusBar(bar)

    def _build_actions(self) -> None:
        """One QAction per command, with its key and its glyph.

        They are window actions, not toolbar actions, so a command whose button is
        in the overflow menu is still reachable by its shortcut - and the palette
        is for discovery.
        """
        for command, description in command_table():
            shortcut = SHORTCUTS.get(command, "")
            action = QAction(description, self)
            action.setIcon(icons.command_icon(command, ICON))
            action.setIconText(short_label(command))  # the button text, if shown
            if shortcut:
                action.setShortcut(shortcut)
                action.setToolTip(f"{description} ({shortcut})")
            else:
                action.setToolTip(description)
            action.triggered.connect(
                lambda _checked=False, name=command: self.run_command(name)
            )
            self.addAction(action)
            self._actions[command] = action

        # macOS lifts Quit into the application menu; elsewhere the role is ignored
        self._actions["quit"].setMenuRole(QAction.MenuRole.QuitRole)

    def _build_toolbar(self) -> None:
        """Every command as a button. The glyph is the label, the tooltip the name.

        Twenty-one buttons with text do not fit a 1280px window: Qt would push the
        tail into a ">>" menu. Icons only, with the description and the key in the
        tooltip. Switch to ToolButtonTextBesideIcon for words instead.
        """
        self.toolbar = QToolBar(_("Commands"), self)
        self.toolbar.setMovable(False)
        self.toolbar.setToolButtonStyle(Qt.ToolButtonStyle.ToolButtonIconOnly)
        self.toolbar.setIconSize(QSize(ICON, ICON))
        self.addToolBar(self.toolbar)

        for command in TOOLBAR:
            if command is None:
                self.toolbar.addSeparator()
            else:
                self.toolbar.addAction(self._actions[command])

    def _build_menus(self) -> None:
        """The usual menus, holding the very actions the toolbar shows.

        Not copies: the same QAction, so one command has one shortcut, one icon and
        one enabled state wherever it appears - and no way for the two to drift.
        """
        bar = self.menuBar()
        for title, commands in menu_table():
            menu = bar.addMenu(title)
            for command in commands:
                if command is None:
                    menu.addSeparator()
                else:
                    menu.addAction(self._actions[command])

    # ---------- commands from the palette ----------

    def _command_palette(self) -> None:
        """The TUI's ':' palette, with the same commands and descriptions."""
        dialog = CommandPalette(self)
        if dialog.exec() != QDialog.DialogCode.Accepted:
            return
        command = dialog.chosen()
        if command:
            self.run_command(command)

    def run_command(self, cmd: str) -> None:
        """Run a command. The table is the TUI's bindings and palette, plus untag."""
        logger.info("palette: %s", cmd)
        handlers = {
            "refresh": self._refresh,
            "scan": self._scan,
            "search": self._search_query,
            "tag": self._tag_selected,
            "untag": self._untag_selected,
            "delete": self._delete_tag,
            "sort_files": self._sort,
            "init_db": self._init_db,
            "config": self._edit_config,
            "stats": self._stats,
            "tree": self._show_tree,
            "prune": self._prune,
            "mount": self._mount,
            "umount": self._umount,
            "command_palette": self._command_palette,
            "help": self._help,
            "quit": self.close,
        }
        handler = handlers.get(cmd)
        if handler is None:
            self._status_label.setText(
                _("'{cmd}' isn't available in the GUI yet").format(cmd=cmd)
            )
            return
        handler()

    # ---------- contents ----------

    def _refresh(self) -> None:
        """Re-read tags, the filesystem panel and the current view after any write."""
        self._tree.invalidate()
        self.tags.reload()
        # The panel no longer notices changes by itself, so it has to be told.
        # getattr: a test may have swapped the panel for a stub without refresh().
        refresh_panel = getattr(self.fs, "refresh", None)
        if refresh_panel is not None:
            refresh_panel()
        if self._search is not None:
            self._run_search(self._search)
            return
        self.open_folder(self._vpath)

    def open_folder(self, path: str) -> None:
        """Show a virtual folder: its subfolders and its files."""
        self._vpath = VirtualTree.normalize(path)
        self._search = None
        self._selection.clear()
        # The preview belongs to the folder: a decode still on its way must not
        # paint over the emptied grid once the listing below has cleared it
        self._preview_token += 1
        self.grid.clear()
        self._queue.clear()
        self._update_status()

        generation = self._tasks.bump()
        self._tasks.run(
            self._tree.listdir,
            on_done=lambda nodes: self._show_nodes(nodes, generation),
            on_failed=lambda error: self._show_error(self._vpath, error),
            args=(self._vpath,),
            generation=generation,
        )

    def _show_nodes(self, nodes: list[Node], generation: int) -> None:
        """Fill the grid: folders first, then files, thumbnails in batches."""
        if generation != self._tasks.generation:
            logger.debug("dropping stale listing for %s (gen %s)", self._vpath, generation)
            return

        for node in nodes:
            if node.is_dir:
                item = QListWidgetItem(node.name)
                item.setData(self.ROLE_VPATH, node.vpath)
                item.setIcon(self._folder_icon())
                item.setToolTip(node.vpath)
                self.grid.addItem(item)
                continue
            item = self._add_file_item(
                node.name,
                node.source_path,
                node.file_id,
                node.mtime,
                node.file_type,
                node.vpath,
            )
            self._queue.append((node.file_id, node.source_path, node.mtime, item))

        self._update_status()
        self._pump(generation)

    def _add_file_item(
        self,
        name: str,
        source: str | None,
        file_id: int | None,
        mtime: float,
        file_type: FileType | None,
        vpath: str | None = None,
    ) -> QListWidgetItem:
        """One grid row for a file: a placeholder icon now, a thumbnail later."""
        item = QListWidgetItem(name)
        item.setData(self.ROLE_VPATH, vpath)
        item.setData(self.ROLE_SOURCE, source)
        item.setData(self.ROLE_FILE_ID, file_id)
        item.setData(self.ROLE_MTIME, mtime)
        item.setIcon(self._placeholder_icon(file_type))
        item.setToolTip(self._file_tooltip(item))
        self.grid.addItem(item)
        return item

    def _pump(self, generation: int) -> None:
        """Hand the pool one batch; its last result asks for the next one.

        Not a zero-timer per batch: that resubmits the next batch the moment this
        one is queued, so a huge folder ends up with the whole folder waiting in the
        pool and BATCH limits nothing. Waiting for the batch to land keeps the pool
        queue at BATCH and wakes the event loop once per batch, not once per row.
        """
        if generation != self._tasks.generation:
            return
        if self._paused:
            # A modal dialog is up: the grid is not looked at, and every icon that
            # lands repaints it. Whoever resumes the pump kicks this chain again.
            return

        batch, self._queue = self._queue[:BATCH], self._queue[BATCH:]
        todo = [(f, s, m, item) for f, s, m, item in batch if f is not None and s]
        if not todo:
            # nothing to wait for: rows without a file id must not stall the queue
            if self._queue:
                QTimer.singleShot(0, lambda: self._pump(generation))
            return

        left = len(todo)

        def landed(item: QListWidgetItem, result: Thumbnail | None) -> None:
            """One row is in: paint it, and start the next batch when it was last."""
            nonlocal left
            if result is not None:
                self._set_icon(item, result)
            left -= 1
            if left <= 0 and self._queue:
                self._pump(generation)

        def failed(error: Exception, item: QListWidgetItem) -> None:
            """A decoder that raised is not a crash: keep the placeholder, count it."""
            logger.warning("thumbnail failed for %s: %s", item.text(), error)
            landed(item, None)

        for file_id, source, mtime, item in todo:
            self._tasks.run(
                self._thumbs.load_result,
                on_done=lambda result, item=item: landed(item, result),
                on_failed=lambda error, item=item: failed(error, item),
                args=(Path(source),),
                kwargs={"file_id": file_id, "mtime": mtime},
                generation=generation,
            )

    def _folder_icon(self) -> QIcon:
        return self.style().standardIcon(QStyle.StandardPixmap.SP_DirIcon)

    def _placeholder_icon(self, file_type: FileType | None) -> QIcon:
        """Type icon shown until a thumbnail lands - and when it never does."""
        return icons.placeholder_for(file_type, self._thumbs.size)

    def _file_tooltip(self, item: QListWidgetItem, reason: str = "") -> str:
        """Name, real path, and - when there is no preview - why."""
        lines = [item.text()]
        source = item.data(self.ROLE_SOURCE)
        if source:
            lines.append(str(source))
        if reason:
            lines.append(reason)
        return "\n".join(lines)

    def _set_icon(self, item: QListWidgetItem, result: Thumbnail) -> None:
        """A decoded thumbnail replaces the placeholder; a failure explains itself."""
        if result.ok:
            item.setIcon(QIcon(QPixmap.fromImage(result.image)))
            item.setToolTip(self._file_tooltip(item))
            return
        item.setToolTip(self._file_tooltip(item, _reason_text(result.reason)))

    def _update_status(self) -> None:
        """Refresh the status bar: current view and how much is selected."""
        if self._search is not None:
            where = _("Search: {query}").format(query=self._search)
        else:
            where = self._vpath
        self.statusBar().showMessage(f"{where} — {self.grid.count()}")
        if self._selection:
            self._status_label.setText(f"{_('Selected:')} {len(self._selection)}")
        else:
            self._status_label.setText(_("No selection"))

    def _show_error(self, path: str, error: Exception) -> None:
        """A listing that failed must be visible, never a silently empty grid."""
        logger.warning("cannot list %s: %s", path, error)
        self._status_label.setText(
            _("Cannot open {path}: {error}").format(path=path, error=error)
        )

    # ---------- commands ----------

    def _target_files(self) -> list[tuple]:
        """(file_id, path) for the grid selection, else the real-FS selection."""
        chosen = [
            (item.data(self.ROLE_FILE_ID), item.data(self.ROLE_SOURCE))
            for item in self.grid.selectedItems()
            if item.data(self.ROLE_FILE_ID) is not None
        ]
        if chosen:
            return chosen

        path = self.fs.current_path()
        if path is None or path.is_dir():
            return []
        entry = self._repo.get_file_by_path(str(path))
        return [(entry.id, entry.filepath)] if entry else []

    def _tag_selected(self) -> None:
        """Add a tag (a path creates missing parents) to the selected files."""
        targets = self._target_files()
        if not targets:
            self._status_label.setText(_("Select a real file first!"))
            return

        name, accepted = QInputDialog.getText(
            self, _("Tag"), _("Enter tag (e.g., vacation/beach)")
        )
        if not accepted or not name.strip():
            return
        for file_id, _path in targets:
            self._repo.add_tag(file_id, name.strip())
        self._refresh()

    def _untag_selected(self) -> None:
        """Remove a tag from the selected files."""
        targets = self._target_files()
        if not targets:
            self._status_label.setText(_("Select a real file first!"))
            return

        name, accepted = QInputDialog.getText(
            self, _("Remove tag from file"), _("Tag path")
        )
        if not accepted or not name.strip():
            return
        for file_id, _path in targets:
            self._repo.remove_tag(file_id, name.strip())
        self._refresh()

    def _delete_tag(self) -> None:
        """Delete the tag selected in the tree. Its children move up one level."""
        item = self.tags.currentItem()
        if item is None or item.data(0, Qt.ItemDataRole.UserRole) == "/":
            self._status_label.setText(_("Select a tag folder in the virtual panel first"))
            return

        # The tree shows sanitized names; the repository needs the raw tag path
        tag_id = item.data(0, TagTree.ROLE_TAG_ID)
        by_id = {tag.id: tag for tag in self._repo.get_all_tags()}
        tag = by_id.get(tag_id)
        if tag is None:
            self._status_label.setText(_("No such path: {path}").format(path=item.text(0)))
            return

        path = tagpaths.path_of(tag, by_id)
        if not self._repo.delete_tag(path):
            self._status_label.setText(_("No such path: {path}").format(path=path))
            return
        self._status_label.setText(_("Deleted tag {path}").format(path="/" + path))
        self._refresh()

    def _scan(self) -> None:
        """Index the folder picked in the real filesystem panel.

        The poll is stopped for the duration: the scan commits the whole time, and
        a refresh every 1.5 s would rebuild the tree and the grid underneath it.
        The task is sticky, so browsing during a long scan cannot swallow its end.
        """
        target = self.fs.current_path()
        if target is None:
            self._status_label.setText(_("Select a real file first!"))
            return

        folder = target if target.is_dir() else target.parent
        self._status_label.setText(_("Scanning {path}...").format(path=folder))
        self._poll.stop()
        indexer = Indexer(self._repo, folder)
        self._tasks.run(
            indexer.run,
            on_done=self._on_scan_done,
            on_failed=self._on_scan_failed,
            sticky=True,
        )

    def _on_scan_done(self, _stats: dict) -> None:
        self._status_label.setText(_("Scan complete!"))
        # The scan itself was the change: re-baseline, so the first poll after it
        # does not refresh a second time on data the refresh below already read
        self._version = None
        self._refresh()
        self._poll.start()

    def _on_scan_failed(self, error: Exception) -> None:
        logger.warning("scan failed: %s", error)
        self._status_label.setText(_("Error: {error}").format(error=error))
        self._version = None
        self._poll.start()

    def _search_query(self) -> None:
        """Ask for a query: a name, or `tag:travel` - the TUI's own rule."""
        query, accepted = QInputDialog.getText(
            self, _("Search"), _("Text in the file name")
        )
        if not accepted or not query.strip():
            return
        self._run_search(query.strip())

    def _run_search(self, query: str) -> None:
        """Search off the event loop and show the hits in the grid."""
        self._search = query
        self._selection.clear()
        self._preview_token += 1
        self.grid.clear()
        self._queue.clear()
        self._update_status()

        if query.startswith("tag:"):
            tag = query[4:]
            work = lambda: self._repo.search_by_tags([tag])
        else:
            work = lambda: self._repo.search_by_name(query)

        generation = self._tasks.bump()
        self._tasks.run(
            work,
            on_done=lambda files: self._show_files(files, generation),
            on_failed=lambda error: self._show_error(query, error),
            generation=generation,
        )

    def _show_files(self, files: list[FileEntry], generation: int) -> None:
        """Fill the grid from search results instead of a folder listing."""
        if generation != self._tasks.generation:
            return
        for entry in files:
            mtime = self._entry_mtime(entry)
            item = self._add_file_item(
                entry.filename, entry.filepath, entry.id, mtime, entry.type
            )
            self._queue.append((entry.id, entry.filepath, mtime, item))
        self._update_status()
        self._pump(generation)

    @staticmethod
    def _entry_mtime(entry: FileEntry) -> float:
        """mtime from the indexed metadata - no stat on the GUI thread."""
        try:
            return float((entry.metadata or {}).get("modified_at") or 0.0)
        except (TypeError, ValueError):
            return 0.0

    def _open_selected(self) -> None:
        selected = self.grid.selectedItems()
        if selected:
            self._open_external(selected[0])

    def _init_db(self) -> None:
        """Create the database - or ask what to do when one is already here.

        Doing nothing was the bug: the user clicks, the file exists, and no window,
        no message, nothing at all.
        """
        if not self._db_path.exists():
            self._use_db(self._db_path)
            self._status_label.setText(_("Database initialized!"))
            return

        box = QMessageBox(self)
        box.setWindowTitle(_("Initialize new database"))
        box.setIcon(QMessageBox.Icon.Question)
        box.setText(_("A database already exists here:"))
        box.setInformativeText(str(self._db_path))
        here = box.addButton(_("Recreate here"), QMessageBox.ButtonRole.AcceptRole)
        elsewhere = box.addButton(
            _("Choose a folder..."), QMessageBox.ButtonRole.ActionRole
        )
        cancel = box.addButton(_("Cancel"), QMessageBox.ButtonRole.RejectRole)
        box.setDefaultButton(here)
        box.setEscapeButton(cancel)
        box.exec()

        clicked = box.clickedButton()
        if clicked is here:
            self._use_db(self._db_path, fresh=True)
            self._status_label.setText(_("Database recreated!"))
        elif clicked is elsewhere:
            self._create_elsewhere()

    def _use_db(self, path: Path, fresh: bool = False) -> None:
        """Point the whole window at another database, empty when `fresh`.

        Nothing is set to None on the way: a task in flight can call back at any
        moment, and it reads self._repo and self._tree.
        """
        path = Path(path)
        if fresh:
            self._wipe(self._repo, path)

        self._repo = self._open_db(path)
        self._db_path = path
        self._tree = VirtualTree(self._repo)
        self._thumbs = ThumbnailCache(size=self._thumbs.size, scope=str(self._db_path))

        self.tags.set_tree(self._tree)
        self._version = None
        self.open_folder("/")
        self._show_preview()

    def _open_db(self, path: Path):
        """Open a repository, waiting out a lock left by a task in flight.

        A scan writes while it runs, so its connection can hold the file for a moment.
        Connections are opened per operation, so the wait is short and the retry is
        only a safety net.
        """
        for _attempt in range(10):
            try:
                return SQLiteRepository(str(path))
            except sqlite3.OperationalError as error:
                if "locked" not in str(error) and "busy" not in str(error):
                    raise
                gc.collect()
                time.sleep(0.05)
        return SQLiteRepository(str(path))

    def _wipe(self, old, path: Path) -> None:
        """Empty the database through SQL, leaving its file alone.

        Deleting the file would leave a brand-new database in `delete` journal mode,
        and switching that to WAL wants an exclusive lock that PRAGMA journal_mode
        does not wait for - that was the `database is locked`. Dropping the objects
        keeps the file, and its WAL mode, exactly where they are, and the schema is
        rebuilt by the repository itself.
        """
        closer = getattr(old, "close", None)
        if closer is not None:
            closer()

        for _attempt in range(10):
            try:
                self._drop_objects(path)
                return
            except sqlite3.OperationalError as error:
                if "locked" not in str(error) and "busy" not in str(error):
                    raise
                gc.collect()
                time.sleep(0.05)
        self._drop_objects(path)

    @staticmethod
    def _drop_objects(path: Path) -> None:
        """Drop every table and view, creating the file when it is not there yet.

        One connection of our own, closed on the way out: the repository opens its
        own per operation, and a handle left behind is one more lock to wait for.
        """
        conn = sqlite3.connect(str(path))
        try:
            # Off first: with it on, the drops would depend on the reference order
            conn.execute("PRAGMA foreign_keys = OFF")
            rows = list(
                conn.execute(
                    "SELECT type, name FROM sqlite_master"
                    " WHERE type IN ('view', 'table') AND name NOT LIKE 'sqlite_%'"
                    " ORDER BY type = 'table'"  # views first, their tables after
                )
            )
            for kind, name in rows:
                conn.execute(f'DROP {kind.upper()} IF EXISTS "{name}"')
            conn.commit()
        finally:
            conn.close()

    def _create_elsewhere(self) -> None:
        """Pick a folder, put a fresh database in it, and work on that one."""
        folder = QFileDialog.getExistingDirectory(
            self, _("Choose a folder for the database"), str(self._db_path.parent)
        )
        if not folder:
            return
        target = Path(folder) / self._db_path.name
        if target.exists() and not self._confirm_overwrite(target):
            return
        self._use_db(target, fresh=True)
        self._status_label.setText(_("Database: {path}").format(path=target))

    def _confirm_overwrite(self, target: Path) -> bool:
        """Yes/no for a database that already sits in the chosen folder."""
        answer = QMessageBox.question(
            self,
            _("Initialize new database"),
            _("A database already exists there:\n{path}\n\nRecreate it?").format(
                path=target
            ),
        )
        return answer == QMessageBox.StandardButton.Yes

    def _edit_config(self) -> None:
        """Open the settings and apply the result when the window closes.

        Deliberately not exec(): that runs a nested event loop, and the poll, the
        thread pool and the dialog end up fighting over the same window.

        The raise_/activateWindow pair is deferred, not immediate. open() already
        raises and activates, but on Windows the owner can take the foreground back
        in that same turn: the dialog ends up behind a window that modality has
        already blocked, and the app looks dead, because Qt drops every click on
        that window. Asking again once the show has settled is what keeps the dialog
        in front. MERET_SETTINGS_MODELESS=1 removes native modality altogether, for
        an A/B run.
        """
        manager = get_config()
        dialog = SettingsDialog(self)

        def finished(result: int) -> None:
            """The dialog is gone: let the poll run again and apply what was chosen."""
            self._paused = False
            self._pump(self._tasks.generation)  # the pause cut the chain in half
            self._poll.start()
            if result != QDialog.DialogCode.Accepted:
                self._status_label.setText(_("Settings unchanged"))
            else:
                apply_settings(dialog.values())
                self._apply_look()
                self._status_label.setText(
                    _("Settings saved to {path}").format(path=manager.config_path)
                )
            dialog.deleteLater()

        def to_front() -> None:
            """Re-ask for the foreground on the next turn of the loop."""
            try:
                dialog.raise_()
                dialog.activateWindow()
            except RuntimeError:
                return  # already deleted: finished() ran and deleteLater() landed

        dialog.finished.connect(finished)
        self._poll.stop()  # no rebuilds underneath a modal window
        self._paused = True  # and no thumbnail batches painting the grid under it

        started = time.perf_counter()
        if os.environ.get("MERET_SETTINGS_MODELESS"):
            dialog.setWindowModality(Qt.WindowModality.NonModal)
            dialog.show()
        else:
            dialog.open()
        logger.info("settings shown in %.0f ms", (time.perf_counter() - started) * 1000)

        QTimer.singleShot(0, to_front)  # see the docstring

    def _apply_look(self) -> None:
        """Re-read what the window shows itself with - and only what actually changed.

        Applying a style re-polishes every widget in the app, which is a visible stall
        with a filled grid, so it is skipped when the theme is the same. The window
        size is left alone (the user may have resized it by hand), and a language
        change waits for a restart, since the strings are already built.
        """
        gui = get_config().config.gui

        if gui.theme != self._theme:
            _apply_theme(QApplication.instance(), gui.theme)
            self._theme = gui.theme
            # The glyphs are painted in the palette colour: re-tint them with it
            for command, action in self._actions.items():
                action.setIcon(icons.command_icon(command, ICON))

        if gui.thumbnail_size != self._thumbs.size:
            self.grid.setIconSize(QSize(gui.thumbnail_size, gui.thumbnail_size))
            self._thumbs = ThumbnailCache(
                size=gui.thumbnail_size, scope=str(self._db_path)
            )

    def _stats(self) -> None:
        """Database statistics, read off the event loop."""
        self._tasks.run(self._repo.get_stats, on_done=self._on_stats)

    def _on_stats(self, stats: dict) -> None:
        self._status_label.setText(
            _("Files: {files}, tags: {tags}").format(
                files=stats.get("total_files", 0), tags=stats.get("total_tags", 0)
            )
        )

    def _show_tree(self) -> None:
        """The tag tree is the virtual panel: reload it and move focus there."""
        self.tags.reload()
        self.tags.setFocus()

    def _prune(self) -> None:
        """Delete tags with no files and no subtags."""
        self._tasks.run(self._repo.prune_orphan_tags, on_done=self._on_pruned)

    def _on_pruned(self, removed: int) -> None:
        self._status_label.setText(_("Tags deleted: {n}").format(n=removed))
        self._refresh()

    def _mount(self) -> None:
        """Start `meret mount` in its own process, so `meret umount` can stop it.

        The backend is deliberately not imported: keeping it out of meret.gui means
        no FUSE or WinFsp import just to open a window.
        """
        default = "M:" if sys.platform == "win32" else str(Path.home() / "meret-tags")
        point, accepted = QInputDialog.getText(
            self, _("Mount virtual filesystem"), _("Where to mount, e.g. M:"), text=default
        )
        if not accepted or not point.strip():
            return
        self._status_label.setText(_("Mounting at {path}...").format(path=point.strip()))
        self._spawn(self._cli("mount", point.strip()))

    def _umount(self) -> None:
        """Stop the mount process. It takes no --db, only the mount point."""
        point, accepted = QInputDialog.getText(
            self, _("Unmount"), _("Mount point, e.g. M: or K:\\tags")
        )
        if not accepted or not point.strip():
            return
        point = point.strip()
        self._run(self._cli("umount", point, with_db=False))
        self._status_label.setText(_("Unmounted {path}").format(path=point))

    def _spawn_cli(self, *args: str) -> None:
        """Run the CLI without waiting - a mount lives until it is unmounted."""
        command = ["meret", *args, "--db", str(self._db_path)]
        logger.info("run: %s", command)
        try:
            subprocess.Popen(command)
        except OSError as e:
            logger.warning("cannot run %s: %s", command, e)
            self._status_label.setText(_("Error: {error}").format(error=e))

    def _run_cli(self, *args: str) -> None:
        """Run the CLI and wait - for a command that ends on its own."""
        command = ["meret", *args]
        logger.info("run: %s", command)
        try:
            done = subprocess.run(command, capture_output=True, text=True)
        except OSError as e:
            logger.warning("cannot run %s: %s", command, e)
            self._status_label.setText(_("Error: {error}").format(error=e))
            return
        if done.returncode != 0:
            logger.warning("exit %s: %s", done.returncode, (done.stderr or "").strip())

    def _cli(self, *args: str, with_db: bool = True) -> list[str]:
        """A `meret` command line.

        `with_db=False` for `umount`: it finds the mount process by the mount point
        alone, so `umount_cmd` declares no --db and typer rejects an unknown option.
        """
        command = ["meret", *args]
        if with_db:
            command += ["--db", str(self._db_path)]
        return command

    def _spawn(self, command: list[str]) -> None:
        """Run without waiting - a mount lives until it is unmounted."""
        logger.info("run: %s", command)
        try:
            subprocess.Popen(command)
        except OSError as e:
            logger.warning("cannot run %s: %s", command, e)
            self._status_label.setText(_("Error: {error}").format(error=e))

    def _run(self, command: list[str]) -> None:
        """Run and wait - for a command that ends on its own."""
        logger.info("run: %s", command)
        try:
            done = subprocess.run(command, capture_output=True, text=True)
        except OSError as e:
            logger.warning("cannot run %s: %s", command, e)
            self._status_label.setText(_("Error: {error}").format(error=e))
            return
        if done.returncode != 0:
            logger.warning("exit %s: %s", done.returncode, (done.stderr or "").strip())

    def _sort(self) -> None:
        """Cycle the real-FS panel sorting: name, then size, then date.

        The header is hidden, so clicking cannot sort and the view's own slot is
        the only way in: sortByColumn, not sort - QTreeView has no sort(). Column
        3 is the date, and the two largest-first modes are the useful ones there.
        """
        modes = [_("name"), _("size"), _("date")]
        self._sort_index = (self._sort_index + 1) % len(modes)
        column = (0, 1, 3)[self._sort_index]
        order = (
            Qt.SortOrder.AscendingOrder
            if self._sort_index == 0
            else Qt.SortOrder.DescendingOrder
        )
        self.fs.sortByColumn(column, order)
        self._status_label.setText(
            _("Sort by: {mode}").format(mode=modes[self._sort_index])
        )

    def _help(self) -> None:
        """Every command with its key: the TUI's footer, as a dialog."""
        descriptions = dict(command_table())
        # Width from the data: "Ctrl+Shift+T" is longer than any fixed 12, and a
        # hardcoded column glues it to the description
        key_width = max((len(key) for key in SHORTCUTS.values()), default=1) + 2

        def line(name: str) -> str:
            key = SHORTCUTS.get(name) or "—"
            return f"    {key:<{key_width}}{descriptions[name]}"

        lines = [f"{_('Navigation')}:", *(line(name) for name in NAVIGATION_COMMANDS)]
        lines.append("")
        lines.append(f"{_('Commands')}:")
        lines.extend(
            line(name) for name, _description in command_table()
            if name not in NAVIGATION_COMMANDS
        )
        QMessageBox.information(self, _("Help"), "\n".join(lines))

    # ---------- preview ----------

    def _show_preview(self) -> None:
        selected = self.grid.selectedItems()
        self._selection = {
            key
            for key in (
                item.data(self.ROLE_VPATH) or item.data(self.ROLE_SOURCE)
                for item in selected
            )
            if key
        }
        self._update_status()

        # Two selections share the folder's generation, so a slow decode could
        # overwrite a newer preview: every request takes its own token instead
        self._preview_token += 1
        token = self._preview_token

        if not selected:
            self.preview.setPixmap(QPixmap())
            self.preview.setText(_("Nothing selected"))
            return

        item = selected[0]
        source = item.data(self.ROLE_SOURCE)
        if not source:
            self.preview.setPixmap(QPixmap())
            self.preview.setText(item.text())
            return

        self._tasks.run(
            self._thumbs.load_result,
            on_done=lambda result, token=token, name=item.text(): self._set_preview(
                result, name, token
            ),
            args=(Path(source),),
            kwargs={
                "file_id": item.data(self.ROLE_FILE_ID),
                "mtime": item.data(self.ROLE_MTIME) or 0.0,
                "size": PREVIEW,
            },
        )

    def _set_preview(self, result: Thumbnail, name: str, token: int) -> None:
        if token != self._preview_token:
            return  # a newer selection has already painted
        if not result.ok:
            self.preview.setPixmap(QPixmap())
            reason = _reason_text(result.reason)
            self.preview.setText(f"{name}\n{reason}" if reason else name)
            return
        self.preview.setPixmap(QPixmap.fromImage(result.image))

    def _open_external(self, item: QListWidgetItem) -> None:
        """Open a file with the system handler, or descend into a folder."""
        source = item.data(self.ROLE_SOURCE)
        if source:
            QDesktopServices.openUrl(QUrl.fromLocalFile(source))
            return
        vpath = item.data(self.ROLE_VPATH)
        if vpath:
            self.open_folder(vpath)

    def _on_real_activated(self, path: str) -> None:
        """A real file: jump to its first tag folder, or hand it to the system."""
        target = Path(path)
        if target.is_dir():
            self._status_label.setText(_("Path: {path}").format(path=target))
            return

        entry = self._repo.get_file_by_path(str(target))
        if entry is None:
            self._status_label.setText(
                _("File is not in the database: {path}").format(path=target)
            )
            return

        if entry.labels:
            known = tagpaths.name_to_path(self._repo.get_all_tags())
            first = known.get(entry.labels[0].casefold())
            if first:
                self.tags.reload()
                self.open_folder("/" + first)
                return
        QDesktopServices.openUrl(QUrl.fromLocalFile(str(target)))

    # ---------- outside changes ----------

    def _check_version(self) -> None:
        """Pick up tagging done by another process (the CLI, a mount, a scan).

        Sticky: every folder change bumps the generation, so a probe that got
        dropped each time would never report anything while the user browses.
        """
        self._tasks.run(self._repo.data_version, self._on_version, sticky=True)

    def _on_version(self, version: int | None) -> None:
        if version is None or version == self._version:
            return
        known = self._version is not None
        self._version = version
        if not known:
            return  # the first reading is just a baseline
        logger.debug("data_version moved to %s, reloading", version)
        self._refresh()

    def closeEvent(self, event) -> None:
        self._poll.stop()
        self._tasks.shutdown()
        super().closeEvent(event)

class TracedApplication(QApplication):
    """An application that counts events and names whatever floods them.

    A freeze the suite cannot reproduce has two shapes, and they need opposite
    fixes: the loop stops turning - one event never returns, or the window system
    never hands the input over - or it turns far too fast, a layout or paint storm.
    The rate tells them apart, and the per-receiver table names the widget that has
    to be fixed.

    The report goes to stderr through print(), never through logging: the CLI may
    have pointed logging at a file, and a diagnostic that can be swallowed is worse
    than none. It is printed by a daemon thread, so the output keeps coming even
    when the GUI thread is the thing that is stuck - a frozen counter is the answer.
    """

    SLOW_MS = 150.0
    REPORT_MS = 1000
    TOP = 6

    def __init__(self, argv: list[str]) -> None:
        super().__init__(argv)
        self._events = 0
        self._worst = 0.0
        self._worst_where = "-"
        self._counts: dict[tuple, int] = {}
        self._chains: dict[tuple, str] = {}
        self._last_report = time.perf_counter()
        self._text = ""

        self._beat = QTimer(self)
        self._beat.setInterval(self.REPORT_MS)
        self._beat.timeout.connect(self._report)
        self._beat.start()

        threading.Thread(target=self._watch, daemon=True).start()

    def notify(self, receiver, event) -> bool:
        """Time every event and count it per receiver, so a storm names itself."""
        started = time.perf_counter()
        try:
            return super().notify(receiver, event)
        finally:
            spent = (time.perf_counter() - started) * 1000
            self._events += 1
            pair = (type(receiver).__name__, type(event).__name__)
            self._counts[pair] = self._counts.get(pair, 0) + 1
            if pair not in self._chains:
                self._chains[pair] = self._chain(receiver)
            if spent > self._worst:
                self._worst = spent
                self._worst_where = f"{pair[0]}/{pair[1]}"
            if spent >= self.SLOW_MS:
                print(
                    f"[watchdog] slow: {pair[1]} took {spent:.0f} ms in"
                    f" {self._chains[pair]}",
                    file=sys.stderr,
                    flush=True,
                )

    @staticmethod
    def _chain(receiver) -> str:
        """The receiver and its ancestors: a busy widget has to name its window."""
        names = []
        node = receiver
        while node is not None and len(names) < 6:
            names.append(type(node).__name__)
            try:
                node = node.parent() if hasattr(node, "parent") else None
            except RuntimeError:
                break  # the C++ object is already gone, e.g. a finished animation
        return " < ".join(names)

    def _report(self) -> None:
        """Once a second, on the GUI thread: compose the line the thread prints."""
        now = time.perf_counter()
        gap = (now - self._last_report) * 1000
        busiest = sorted(self._counts.items(), key=lambda item: item[1], reverse=True)
        lines = [
            (
                f"{self._events} events/s | worst {self._worst:.0f} ms"
                f" ({self._worst_where}) | tick gap {gap:.0f} ms"
                f" | active {self._name_of(self.activeWindow())}"
                f" | modal {self._name_of(self.activeModalWidget())}"
                f" | shown {[type(w).__name__ for w in self.topLevelWidgets() if w.isVisible()]}"
            )
        ]
        for pair, count in busiest[: self.TOP]:
            lines.append(f"    {count:>8}  {pair[1]} in {self._chains[pair]}")
        self._text = "\n".join(lines)

        self._events = 0
        self._worst = 0.0
        self._worst_where = "-"
        self._counts = {}
        self._chains = {}
        self._last_report = now

    def _watch(self) -> None:
        """A daemon thread: keep printing while the GUI thread cannot."""
        while True:
            time.sleep(self.REPORT_MS / 1000 * 2)
            if self._text:
                print(f"[watchdog]\n{self._text}", file=sys.stderr, flush=True)

    @staticmethod
    def _name_of(widget) -> str:
        """A widget's class, or a dash when there is none."""
        return "-" if widget is None else type(widget).__name__

    
def run_gui(db_path: Path, argv: list[str] | None = None) -> int:
    """Open the database, show the window, run the event loop. Returns the exit code.

    With MERET_WATCHDOG=1 the run is traced: every thread's Python stack is dumped
    every three seconds, and the application counts and times every event. A freeze
    the suite cannot reproduce leaves no other trace, and "the loop stopped" and
    "the loop is spinning" need opposite fixes - the trace tells them apart.
    """
    watch = bool(os.environ.get("MERET_WATCHDOG"))
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