"""The tag hierarchy as folders, filled lazily and off the GUI thread."""

import logging

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import QTreeWidget, QTreeWidgetItem, QWidget

from meret.gui.workers import TaskRunner
from meret.vfs.tree import Node, VirtualTree

logger = logging.getLogger(__name__)


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
        self, tree: VirtualTree | None, tasks: TaskRunner, parent: QWidget | None = None
    ) -> None:
        super().__init__(parent)
        # None only while a database switch is in the air
        self._tree: VirtualTree | None = tree
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

        Nested defs, not lambdas: TaskRunner.run takes Callable[..., Any], so mypy
        has nothing to infer a lambda's arguments from.
        """
        assert self._tree is not None
        item.setData(0, self.LOADING, True)

        def done(nodes: list[Node]) -> None:
            self._filled(item, nodes, generation)

        def failed(error: Exception) -> None:
            self._failed(item, error, generation)

        self._tasks.run(
            self._tree.listdir, on_done=done, on_failed=failed, args=(vpath,), sticky=True
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
