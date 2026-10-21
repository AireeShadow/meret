"""The Qt front-end: window, panels and the event loop.

Qt objects live on the GUI thread only. Anything that touches SQLite or decodes
an image goes through TaskRunner and comes back as a signal. The tag panel reads
the same VirtualTree a mount would serve, so the two can never disagree.
"""

import logging
import sys
from pathlib import Path
from typing import List, Optional

from PySide6.QtCore import QSize, Qt, QTimer, QUrl, Signal
from PySide6.QtGui import QColor, QDesktopServices, QIcon, QPalette, QPixmap
from PySide6.QtWidgets import (
    QApplication,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QSplitter,
    QStatusBar,
    QStyle,
    QTreeWidget,
    QTreeWidgetItem,
    QWidget,
)

from meret.core.config import get_config
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
            parent.addChild(item)
            item.addChild(QTreeWidgetItem([""]))  # lazy-expansion placeholder

    def _on_expanded(self, item: QTreeWidgetItem) -> None:
        if item.data(0, self.FILLED):
            return
        item.takeChildren()
        self._add(item, item.data(0, Qt.ItemDataRole.UserRole))
        item.setData(0, self.FILLED, True)

    def _on_current(self, current: Optional[QTreeWidgetItem], _previous) -> None:
        if current is not None:
            self.tag_selected.emit(current.data(0, Qt.ItemDataRole.UserRole) or "/")


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
        self._thumbs = ThumbnailCache(size=config.thumbnail_size)
        self._tasks = TaskRunner(self)
        self._queue: List[tuple] = []
        self._selection: set[str] = set()
        self._vpath = "/"
        self._version: Optional[int] = None

        # Widgets next
        self._build_ui()

        self.setWindowTitle(_("meret"))
        self.resize(config.window_width, config.window_height)
        _apply_theme(QApplication.instance(), config.theme)

        # Outside changes: the tree is invalidated, never re-read per stat
        self._poll = QTimer(self)
        self._poll.setInterval(POLL_MS)
        self._poll.timeout.connect(self._check_version)
        self._poll.start()

        # First paint - nothing may come after this
        self.open_folder("/")

    def _build_ui(self) -> None:
        splitter = QSplitter(Qt.Orientation.Horizontal, self)

        self.tags = TagTree(self._tree, self)
        self.tags.setMinimumWidth(180)
        self.tags.tag_selected.connect(self.open_folder)
        splitter.addWidget(self.tags)

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

    # ---------- contents ----------

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

    def _show_error(self, path: str, error: Exception) -> None:
        """A listing that failed must be visible, never a silently empty grid."""
        logger.warning("cannot list %s: %s", path, error)
        self._status_label.setText(
            _("Cannot open {path}: {error}").format(path=path, error=error)
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

    # ---------- preview ----------

    def _show_preview(self) -> None:
        selected = self.grid.selectedItems()
        self._selection = {
            vpath for vpath in (item.data(self.ROLE_VPATH) for item in selected) if vpath
        }
        self._update_status()

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
            on_done=lambda image: self._set_preview(image, item.text()),
            args=(Path(source),),
            kwargs={
                "file_id": item.data(self.ROLE_FILE_ID),
                "mtime": item.data(self.ROLE_MTIME) or 0.0,
                "size": PREVIEW,
            },
        )

    def _set_preview(self, image, name: str) -> None:
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
        self._tree.invalidate()
        self.tags.reload()
        self.open_folder(self._vpath)

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