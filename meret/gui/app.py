"""The Qt front-end: window, panels and the event loop.

Qt objects live on the GUI thread only. Anything that touches SQLite or decodes
an image goes through TaskRunner and comes back as a signal. The tag panel reads
the same VirtualTree a mount would serve, so the two can never disagree, and the
real filesystem panel is plain Qt - it only picks a path to scan or to tag.

Mounting and unmounting shell out to `meret mount` / `meret umount` instead of
importing a backend, so opening a window never drags FUSE or WinFsp in.
"""

import logging
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional

from PySide6.QtCore import QDir, QSize, Qt, QTimer, QUrl, Signal
from PySide6.QtGui import QAction, QColor, QDesktopServices, QIcon, QPalette, QPixmap
from PySide6.QtWidgets import (
    QApplication,
    QDialog,
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
from meret.gui.commands import SHORTCUTS, CommandPalette, command_table, short_label
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

#: Toolbar buttons, in order. None is a separator. Everything else is one key away.
TOOLBAR: tuple = (
    "refresh",
    "scan",
    "search",
    None,
    "tag",
    "untag",
    "delete",
    None,
    "open_file",
    None,
    "command_palette",
    "help",
    None,
    "quit",
)

#: The help dialog groups these under Navigation, as the TUI's help does.
NAVIGATION_COMMANDS = ("home", "up", "sort_files", "select_drive", "open_file")

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


def _apply_theme(app: Optional[QApplication], theme: str) -> None:
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

    Children are read only when a row is expanded: a library with thousands of
    tags must not cost a query per tag on startup.
    """

    tag_selected = Signal(str)
    FILLED = Qt.ItemDataRole.UserRole + 10
    ROLE_TAG_ID = Qt.ItemDataRole.UserRole + 11

    def __init__(self, tree: VirtualTree, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self._tree = tree
        self.setHeaderHidden(True)
        self.itemExpanded.connect(self._on_expanded)
        self.currentItemChanged.connect(self._on_current)

    def reload(self) -> None:
        """Rebuild from the root."""
        self.clear()
        self._add(self.invisibleRootItem(), "/")

    def _add(self, parent: QTreeWidgetItem, vpath: str) -> None:
        for node in self._tree.listdir(vpath):
            if not node.is_dir:
                continue
            item = QTreeWidgetItem([node.name])
            item.setData(0, Qt.ItemDataRole.UserRole, node.vpath)
            item.setData(0, self.ROLE_TAG_ID, node.tag_id)
            parent.addChild(item)
            item.addChild(QTreeWidgetItem([""]))

    def _on_expanded(self, item: QTreeWidgetItem) -> None:
        if item.data(0, self.FILLED):
            return
        item.takeChildren()
        self._add(item, item.data(0, Qt.ItemDataRole.UserRole))
        item.setData(0, self.FILLED, True)

    def _on_current(self, current: Optional[QTreeWidgetItem], _previous) -> None:
        if current is not None:
            self.tag_selected.emit(current.data(0, Qt.ItemDataRole.UserRole) or "/")


class RealFsPanel(QTreeView):
    """The real filesystem: where a folder to scan is picked.

    Qt's own model, so nothing here needs a database connection or a pool thread.
    The root is remembered by hand: the Up action has to know where it is.
    """

    path_activated = Signal(str)

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self._root = Path.home()
        self._model = QFileSystemModel(self)
        self._model.setFilter(
            QDir.Filter.AllDirs | QDir.Filter.Files | QDir.Filter.NoDotAndDotDot
        )
        self.setModel(self._model)
        self.setHeaderHidden(True)
        # Sortable, even though the hidden header cannot be clicked: sortByColumn
        # needs the view in the sorting mode to keep the indicator and the model
        # in step
        self.setSortingEnabled(True)
        for column in (1, 2, 3):  # size, type, date - the name column is enough
            self.hideColumn(column)
        self.doubleClicked.connect(self._on_double)

    def set_root(self, path: Path) -> None:
        """Point the panel at `path` and remember it."""
        self._root = Path(path)
        self.setRootIndex(self._model.setRootPath(str(path)))

    def root_path(self) -> Path:
        """Where the panel is rooted right now."""
        return self._root

    def current_path(self) -> Optional[Path]:
        index = self.currentIndex()
        if not index.isValid():
            return None
        return Path(self._model.filePath(index))

    def _on_double(self, index) -> None:
        self.path_activated.emit(self._model.filePath(index))


class MainWindow(QMainWindow):
    """Browse the tag tree, the files under one tag, and a preview."""

    ROLE_VPATH = Qt.ItemDataRole.UserRole
    ROLE_SOURCE = Qt.ItemDataRole.UserRole + 1
    ROLE_MTIME = Qt.ItemDataRole.UserRole + 2
    ROLE_FILE_ID = Qt.ItemDataRole.UserRole + 3

    def __init__(self, repo, db_path, parent: Optional[QWidget] = None) -> None:
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
        self._queue: List[tuple] = []
        self._selection: set[str] = set()
        self._vpath = "/"
        self._search: Optional[str] = None
        self._version: Optional[int] = None
        self._preview_token = 0
        self._sort_index = 0
        self._actions: Dict[str, QAction] = {}

        # Widgets next
        self._build_ui()
        self._build_actions()
        self._build_toolbar()

        self.setWindowTitle(_("meret"))
        self.resize(config.window_width, config.window_height)
        _apply_theme(QApplication.instance(), config.theme)

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

        self.tags = TagTree(self._tree, self)
        self.tags.setMinimumWidth(180)
        self.tags.tag_selected.connect(self.open_folder)

        self.fs = RealFsPanel(self)
        self.fs.path_activated.connect(self._on_real_activated)
        self.fs.set_root(self._db_path.parent)

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
        """One QAction per command, with its key. The window owns every one.

        They are window actions, not toolbar actions, so a command that has no
        button is still reachable by its shortcut - the palette is for discovery.
        """
        for command, description in command_table():
            shortcut = SHORTCUTS.get(command, "")
            action = QAction(description, self)
            action.setIconText(short_label(command))  # the button text
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

    def _build_toolbar(self) -> None:
        """Buttons for the frequent commands. The rest is a key or a palette away."""
        self.toolbar = QToolBar(_("Commands"), self)
        self.toolbar.setMovable(False)
        # The actions carry no icons: without this Qt reserves icon-sized buttons
        self.toolbar.setToolButtonStyle(Qt.ToolButtonStyle.ToolButtonTextOnly)
        self.addToolBar(self.toolbar)

        for command in TOOLBAR:
            if command is None:
                self.toolbar.addSeparator()
            else:
                self.toolbar.addAction(self._actions[command])

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
            "open_file": self._open_selected,
            "home": self._go_home,
            "up": self._go_up,
            "sort_files": self._sort,
            "select_drive": self._select_drive,
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
        """Re-read tags and the current view after any write."""
        self._tree.invalidate()
        self.tags.reload()
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

    def _show_nodes(self, nodes: List[Node], generation: int) -> None:
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
        source: Optional[str],
        file_id: Optional[int],
        mtime: float,
        file_type: Optional[FileType],
        vpath: Optional[str] = None,
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
        """Hand the pool one batch, then come back for the next."""
        if generation != self._tasks.generation:
            return
        batch, self._queue = self._queue[:BATCH], self._queue[BATCH:]
        for file_id, source, mtime, item in batch:
            if file_id is None or not source:
                continue
            self._tasks.run(
                self._thumbs.load_result,
                on_done=lambda result, item=item: self._set_icon(item, result),
                args=(Path(source),),
                kwargs={"file_id": file_id, "mtime": mtime},
                generation=generation,
            )
        if self._queue:
            QTimer.singleShot(0, lambda: self._pump(generation))

    def _folder_icon(self) -> QIcon:
        return self.style().standardIcon(QStyle.StandardPixmap.SP_DirIcon)

    def _placeholder_icon(self, file_type: Optional[FileType]) -> QIcon:
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

    def _target_files(self) -> List[tuple]:
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
            work = lambda: self._repo.search_by_tags([tag])  # noqa: E731
        else:
            work = lambda: self._repo.search_by_name(query)  # noqa: E731

        generation = self._tasks.bump()
        self._tasks.run(
            work,
            on_done=lambda files: self._show_files(files, generation),
            on_failed=lambda error: self._show_error(query, error),
            generation=generation,
        )

    def _show_files(self, files: List[FileEntry], generation: int) -> None:
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
        """The window's repository is the database: make sure it is on disk."""
        if not self._db_path.exists():
            SQLiteRepository(str(self._db_path))
        self._version = None
        self._refresh()
        self._status_label.setText(_("Database initialized!"))

    def _edit_config(self) -> None:
        """Open the config file in the system's editor, creating it if needed."""
        manager = get_config()
        if not manager.config_path.exists():
            manager.save()  # defaults, so the editor has something to show
        QDesktopServices.openUrl(QUrl.fromLocalFile(str(manager.config_path)))
        self._status_label.setText(_("Opened {path}").format(path=manager.config_path))

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
        self._spawn_cli("mount", point.strip())

    def _umount(self) -> None:
        """Stop the mount process. `meret umount` takes no --db, only the point."""
        point, accepted = QInputDialog.getText(
            self, _("Unmount"), _("Mount point, e.g. M: or K:\\tags")
        )
        if not accepted or not point.strip():
            return
        point = point.strip()
        self._run_cli("umount", point)
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

    def _go_home(self) -> None:
        """Point the real-FS panel at the home directory."""
        home = Path.home()
        self.fs.set_root(home)
        self._status_label.setText(_("Path: {path}").format(path=home))

    def _go_up(self) -> None:
        """One level up. At a drive root there is nowhere up, so pick a drive."""
        root = self.fs.root_path()
        parent = root.parent
        if parent == root:
            self._select_drive()
            return
        self.fs.set_root(parent)
        self._status_label.setText(_("Path: {path}").format(path=parent))

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

    def _select_drive(self) -> None:
        """Pick a drive. Windows only - the TUI says the same."""
        if sys.platform != "win32":
            self._status_label.setText(_("Drive selection only available on Windows"))
            return

        drives = [info.absoluteFilePath() for info in QDir.drives()]
        if not drives:
            return
        chosen, accepted = QInputDialog.getItem(
            self, _("Select drive"), _("Select Drive:"), drives, 0, False
        )
        if not accepted or not chosen:
            return
        self.fs.set_root(Path(chosen))
        self._status_label.setText(_("Switched to {drive}").format(drive=chosen))

    def _help(self) -> None:
        """Every command with its key: the TUI's footer, as a dialog."""
        descriptions = dict(command_table())

        def line(name: str) -> str:
            key = SHORTCUTS.get(name) or "—"
            return f"    {key:<12}{descriptions[name]}"

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

    def _on_version(self, version: Optional[int]) -> None:
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


def run_gui(db_path: Path, argv: Optional[List[str]] = None) -> int:
    """Open the database, show the window, run the event loop. Returns the exit code."""
    repo = SQLiteRepository(str(db_path))
    app = QApplication.instance() or QApplication(argv or [sys.argv[0]])
    app.setApplicationName("meret")

    window = MainWindow(repo, db_path)
    window.show()
    try:
        return app.exec()
    finally:
        window.close()