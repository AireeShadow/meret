"""The Qt front-end: window, panels and the event loop.

Qt objects live on the GUI thread only. Anything that touches SQLite or decodes
an image goes through TaskRunner and comes back as a signal. The tag panel reads
the same VirtualTree a mount would serve, so the two can never disagree, and the
real filesystem panel is plain Qt - it only picks a path to scan or to tag.
"""

import logging
import sys
from pathlib import Path
from typing import List, Optional

from PySide6.QtCore import QDir, QSize, Qt, QTimer, QUrl, Signal
from PySide6.QtGui import QAction, QColor, QDesktopServices, QIcon, QPalette, QPixmap
from PySide6.QtWidgets import (
    QApplication,
    QFileSystemModel,
    QGroupBox,
    QInputDialog,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
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
from meret.core.sqlite_repo import SQLiteRepository
from meret.gui.thumbnails import ThumbnailCache
from meret.gui.workers import TaskRunner
from meret.vfs.tree import Node, VirtualTree

logger = logging.getLogger(__name__)

#: Thumbnails requested per round - a huge folder must not flood the pool queue.
BATCH = 24
#: How often the database's data_version is checked for outside changes.
POLL_MS = 1500
#: Preview size, in pixels.
PREVIEW = 512

_DARK_PALETTE = {
    "Window": "#1e1f22",
    "WindowText": "#e6e6e6",
    "Base": "#25262a",
    "AlternateBase": "#2b2d31",
    "Text": "#e6e6e6",
    "Button": "#2b2d31",
    "ButtonText": "#e6e6e6",
    "Highlight": "#3d6ea5",
    "HighlightedText": "#ffffff",
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
    app.setPalette(palette)


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
    """

    path_activated = Signal(str)

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self._model = QFileSystemModel(self)
        self._model.setFilter(
            QDir.Filter.AllDirs | QDir.Filter.Files | QDir.Filter.NoDotAndDotDot
        )
        self.setModel(self._model)
        self.setHeaderHidden(True)
        for column in (1, 2, 3):  # size, type, date - the name column is enough
            self.hideColumn(column)
        self.doubleClicked.connect(self._on_double)

    def set_root(self, path: Path) -> None:
        self.setRootIndex(self._model.setRootPath(str(path)))

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
        self._version: Optional[int] = None
        self._preview_token = 0

        # Widgets next
        self._build_ui()
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

    def _build_toolbar(self) -> None:
        """The commands the TUI had, as buttons. Labels reuse the catalog."""
        self.toolbar = QToolBar(_("Commands"), self)
        self.toolbar.setMovable(False)
        self.addToolBar(self.toolbar)

        def add(label: str, handler, shortcut: str) -> None:
            action = QAction(label, self)
            action.setShortcut(shortcut)
            action.triggered.connect(handler)
            self.toolbar.addAction(action)

        add(_("Refresh"), self._refresh, "F5")
        add(_("Scan"), self._scan, "F6")
        self.toolbar.addSeparator()
        add(_("Tag selected file"), self._tag_selected, "Ctrl+T")
        add(_("Remove tag from file"), self._untag_selected, "Ctrl+Shift+T")
        add(_("Delete tag"), self._delete_tag, "Ctrl+D")
        self.toolbar.addSeparator()
        add(_("Open file"), self._open_selected, "Ctrl+O")
        add(_("Quit"), self.close, "Ctrl+Q")

    # ---------- contents ----------

    def _refresh(self) -> None:
        """Re-read tags and the current folder after any write."""
        self._tree.invalidate()
        self.tags.reload()
        self.open_folder(self._vpath)

    def open_folder(self, path: str) -> None:
        """Show a virtual folder: its subfolders and its files."""
        self._vpath = VirtualTree.normalize(path)
        self._selection.clear()
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
            item = QListWidgetItem(node.name)
            item.setData(self.ROLE_VPATH, node.vpath)
            if node.is_dir:
                item.setIcon(self._folder_icon())
                self.grid.addItem(item)
                continue
            item.setData(self.ROLE_FILE_ID, node.file_id)
            item.setData(self.ROLE_SOURCE, node.source_path)
            item.setData(self.ROLE_MTIME, node.mtime)
            item.setToolTip(f"{node.name}\n{node.source_path}")
            self.grid.addItem(item)
            self._queue.append((node.file_id, node.source_path, node.mtime, item))

        self._update_status()
        self._pump(generation)

    def _pump(self, generation: int) -> None:
        """Hand the pool one batch, then come back for the next."""
        if generation != self._tasks.generation:
            return
        batch, self._queue = self._queue[:BATCH], self._queue[BATCH:]
        for file_id, source, mtime, item in batch:
            if file_id is None or not source:
                continue
            self._tasks.run(
                self._thumbs.load,
                on_done=lambda image, item=item: self._set_icon(item, image),
                args=(Path(source),),
                kwargs={"file_id": file_id, "mtime": mtime},
                generation=generation,
            )
        if self._queue:
            QTimer.singleShot(0, lambda: self._pump(generation))

    def _folder_icon(self) -> QIcon:
        return self.style().standardIcon(QStyle.StandardPixmap.SP_DirIcon)

    def _set_icon(self, item: QListWidgetItem, image) -> None:
        if image is not None and not image.isNull():
            item.setIcon(QIcon(QPixmap.fromImage(image)))

    def _update_status(self) -> None:
        """Refresh the status bar: current folder and how much is selected."""
        self.statusBar().showMessage(f"{self._vpath} — {self.grid.count()}")
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
        """Index the folder picked in the real filesystem panel."""
        target = self.fs.current_path()
        if target is None:
            self._status_label.setText(_("Select a real file first!"))
            return

        folder = target if target.is_dir() else target.parent
        self._status_label.setText(_("Scanning {path}...").format(path=folder))
        indexer = Indexer(self._repo, folder)
        self._tasks.run(
            indexer.run, on_done=self._on_scan_done, on_failed=self._on_scan_failed
        )

    def _on_scan_done(self, _stats: dict) -> None:
        self._status_label.setText(_("Scan complete!"))
        self._refresh()

    def _on_scan_failed(self, error: Exception) -> None:
        logger.warning("scan failed: %s", error)
        self._status_label.setText(_("Error: {error}").format(error=error))

    def _open_selected(self) -> None:
        selected = self.grid.selectedItems()
        if selected:
            self._open_external(selected[0])

    # ---------- preview ----------

    def _show_preview(self) -> None:
        selected = self.grid.selectedItems()
        self._selection = {
            vpath for vpath in (item.data(self.ROLE_VPATH) for item in selected) if vpath
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
            self._thumbs.load,
            on_done=lambda image, token=token, name=item.text(): self._set_preview(
                image, name, token
            ),
            args=(Path(source),),
            kwargs={
                "file_id": item.data(self.ROLE_FILE_ID),
                "mtime": item.data(self.ROLE_MTIME) or 0.0,
                "size": PREVIEW,
            },
        )

    def _set_preview(self, image, name: str, token: int) -> None:
        if token != self._preview_token:
            return  # a newer selection has already painted
        if image is None or image.isNull():
            self.preview.setPixmap(QPixmap())
            self.preview.setText(name)
            return
        self.preview.setPixmap(QPixmap.fromImage(image))

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
        """Pick up tagging done by another process (the CLI, a mount, a scan)."""
        self._tasks.run(self._repo.data_version, self._on_version)

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