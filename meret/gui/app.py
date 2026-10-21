"""The Qt front-end: window, panels and the event loop.

Qt objects live on the GUI thread only. Anything that touches SQLite or decodes
an image goes through TaskRunner and comes back as a signal. The tag panel reads
the same VirtualTree a mount would serve, so the two can never disagree.
"""

import logging
import sys
from pathlib import Path
from typing import Any, List, Optional

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
    QTreeWidget,
    QTreeWidgetItem,
)

from meret.core.config import get_config, GUIConfig
from meret.core.locale import _
from meret.core.sqlite_repo import SQLiteRepository
from meret.gui.thumbnails import ThumbnailCache
from meret.gui.workers import TaskRunner
from meret.vfs.tree import VirtualTree

logger = logging.getLogger(__name__)

#: Thumbnails requested per round — a huge folder must not flood the pool queue.
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

    def __init__(self, tree: VirtualTree, parent: Optional[QTreeWidget] = None) -> None:
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

    ROLE_SOURCE = Qt.ItemDataRole.UserRole + 1
    ROLE_MTIME = Qt.ItemDataRole.UserRole + 2

    def __init__(self, repo, db_path) -> None:
        super().__init__()

        # State first: open_folder() is called on the last line of __init__
        self._repo = repo
        self._db_path = Path(db_path)
        self._selection: set[str] = set()
        self._current_path = "/"
        self._tree = VirtualTree(repo)
        self._thumbnails = ThumbnailCache()
        self._runner = TaskRunner()
        self._config = GUIConfig()

        # Widgets next
        self.grid = QListWidget()
        self._status_label = QLabel(_("No selection"))
        self._build_layout()

        self.setWindowTitle(_("meret"))
        self.resize(self._config.window_width, self._config.window_height)

        # First paint - nothing may come after this
        self.open_folder("/")

    def _build_ui(self) -> None:
        splitter = QSplitter(Qt.Orientation.Horizontal, self)

        self.tags = TagTree(self.tree, self)
        self.tags.setMinimumWidth(180)
        self.tags.tag_selected.connect(self.open_folder)
        splitter.addWidget(self.tags)

        self.grid = QListWidget(self)
        self.grid.setViewMode(QListWidget.ViewMode.IconMode)
        self.grid.setResizeMode(QListWidget.ResizeMode.Adjust)
        self.grid.setMovement(QListWidget.Movement.Static)
        self.grid.setUniformItemSizes(True)
        self.grid.setSpacing(6)
        self.grid.setIconSize(QSize(self.thumbs.size, self.thumbs.size))
        self.grid.itemSelectionChanged.connect(self._show_preview)
        self.grid.itemDoubleClicked.connect(self._open_external)
        splitter.addWidget(self.grid)

        self.preview = QLabel(_("Nothing selected"), self)
        self.preview.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.preview.setWordWrap(True)
        self.preview.setMinimumWidth(320)
        splitter.addWidget(self.preview)

        splitter.setStretchFactor(0, 0)
        splitter.setStretchFactor(1, 3)
        splitter.setStretchFactor(2, 2)
        self.setCentralWidget(splitter)
        self.setStatusBar(QStatusBar(self))

    # ---------- contents ----------

    def open_folder(self, path: str) -> None:
        """Fill the grid with the children of a virtual folder."""
        self._current_path = path
        self._selection.clear()
        self.grid.clear()

        try:
            names = self._tree.listdir(path)
        except Exception as error:  # noqa: BLE001 - reported in the status bar
            logger.debug("cannot list %s", path, exc_info=True)
            self._status_label.setText(
                _("Cannot open {path}: {error}").format(path=path, error=error)
            )
            return

        for name in sorted(names):
            child = f"{path.rstrip('/')}/{name}" if path != "/" else f"/{name}"
            item = QListWidgetItem(name)
            item.setData(Qt.ItemDataRole.UserRole, child)
            self.grid.addItem(item)
            self._schedule_thumbnail(child, item)

        self._update_status()

    def _show_nodes(self, nodes: List[Any]) -> None:
        generation = self.tasks.generation
        for node in nodes:
            if node.is_dir or node.file_id is None or not node.source_path:
                continue
            item = QListWidgetItem(node.name)
            item.setData(Qt.ItemDataRole.UserRole, node.file_id)
            item.setData(self.ROLE_SOURCE, node.source_path)
            item.setData(self.ROLE_MTIME, node.mtime)
            item.setToolTip(f"{node.name}\n{node.source_path}")
            self.grid.addItem(item)
            self._queue.append((node.file_id, node.source_path, node.mtime, item))

        self.statusBar().showMessage(f"{self.vpath} — {self.grid.count()}")
        self._pump(generation)

    def _pump(self, generation: int) -> None:
        """Hand the pool one batch, then come back for the next."""
        if generation != self.tasks.generation:
            return
        batch, self._queue = self._queue[:BATCH], self._queue[BATCH:]
        for file_id, source, mtime, item in batch:
            self.tasks.run(
                self.thumbs.load,
                on_done=lambda image, item=item: self._set_icon(item, image),
                args=(Path(source),),
                kwargs={"file_id": file_id, "mtime": mtime},
                generation=generation,
            )
        if self._queue:
            QTimer.singleShot(0, lambda: self._pump(generation))

    # ---------- preview ----------

    def _set_icon(self, item: QListWidgetItem, image) -> None:
        if image is not None and not image.isNull():
            item.setIcon(QIcon(QPixmap.fromImage(image)))

    def _show_preview(self) -> None:
        selected = self.grid.selectedItems()
        if not selected:
            self.preview.setPixmap(QPixmap())
            self.preview.setText(_("Nothing selected"))
            return

        item = selected[0]
        source = item.data(self.ROLE_SOURCE)
        if not source:
            return
        self.tasks.run(
            self.thumbs.load,
            on_done=lambda image: self._set_preview(image, item.text()),
            args=(Path(source),),
            kwargs={
                "file_id": item.data(Qt.ItemDataRole.UserRole),
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
        source = item.data(self.ROLE_SOURCE)
        if source:
            QDesktopServices.openUrl(QUrl.fromLocalFile(source))

    # ---------- outside changes ----------

    def _check_version(self) -> None:
        """Pick up tagging done by another process (the CLI, a mount, a scan)."""
        self.tasks.run(self.repo.data_version, self._on_version)

    def _on_version(self, version: Optional[int]) -> None:
        if version is None or version == self._version:
            return
        known = self._version is not None
        self._version = version
        if not known:
            return  # the first reading is just a baseline
        logger.debug("data_version moved to %s, reloading", version)
        self.tree.invalidate()
        self.tags.reload()
        self.open_folder(self.vpath)

    def closeEvent(self, event) -> None:
        self._poll.stop()
        self.tasks.shutdown()
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
        window.tasks.shutdown()