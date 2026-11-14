"""The real-filesystem panel: one folder at a time, never the drive list."""

import logging
import time
from pathlib import Path

from PySide6.QtCore import QDir, QModelIndex, Qt, QTimer, Signal
from PySide6.QtWidgets import QAbstractItemView, QFileSystemModel, QTreeView, QWidget

from meret.core.debug import enabled

logger = logging.getLogger(__name__)


class RealFsPanel(QTreeView):
    """The real filesystem, one root at a time.

    Qt's own model, so nothing here needs a database connection or a pool thread.
    The view is rooted at ONE folder, never at the model's own root: that root is the
    drive list, and building it asks Windows for every drive's label and free space -
    a dead mapped drive answers after ~5.5 s, on the GUI thread, in a single event.

    This panel does not probe the drives it is offered, and that is deliberate. It
    used to - a thread per drive asking os.path.exists(), later GetDriveTypeW() - so
    that a dead drive could be greyed out in the window's selector. Both calls can
    fault natively on a disconnected mapped drive: an access violation inside the
    network provider, which no except clause can catch and no lock can prevent. The
    drives come from QDir.drives(), names only, and a dead one is paid for when
    someone selects it - a deliberate click, never a probe on a timer.
    """

    path_activated = Signal(str)

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._started = time.monotonic()
        # Guard against a nested event loop starting a second listing on top of one
        # that is still running: that is how the queue stops draining.
        self._refreshing = False
        # The one folder the panel is rooted at. Never "" - see the class docstring.
        self._root_path = QDir.homePath()
        self._model = QFileSystemModel(self)
        self._model.setFilter(QDir.Filter.AllDirs | QDir.Filter.Files | QDir.Filter.NoDotAndDotDot)
        # The watcher is the freeze: QFileSystemModel watches every directory it has
        # ever listed, and a change anywhere in one of them makes it re-read that
        # directory on the GUI thread with a stat per entry - 6.4 seconds in a single
        # event, measured. --debug watch_fs skips the options and leaves the stock
        # model, i.e. exactly what the panel did before the freeze was found.
        if not enabled("watch_fs"):
            self._model.setOption(QFileSystemModel.Option.DontWatchForChanges, True)
            self._model.setOption(QFileSystemModel.Option.DontResolveSymlinks, True)
            # A directory icon is an SHGetFileInfo call per entry on Windows
            self._model.setOption(QFileSystemModel.Option.DontUseCustomDirectoryIcons, True)
        self.setModel(self._model)
        self.setHeaderHidden(True)
        # Rows are one line high and never wrap, so the view can skip asking every
        # item for its height - which matters with thousands of rows under a root
        self.setUniformRowHeights(True)
        # Sortable, even though the hidden header cannot be clicked: sortByColumn
        # needs the view in the sorting mode to keep the indicator and the model
        # in step
        self.setSortingEnabled(True)
        # ...which adopts the header's default indicator, descending: Z to A, and the
        # drives from the last letter down. Name ascending is the window's first mode.
        self.sortByColumn(0, Qt.SortOrder.AscendingOrder)
        for column in (1, 2, 3):  # size, type, date - the name column is enough
            self.hideColumn(column)
        self.doubleClicked.connect(self._on_double)
        # Every listing is resolved on the GUI thread, so the cost of revealing a
        # folder is the size of its ancestors. Naming them in the log beats guessing.
        self._model.directoryLoaded.connect(self._log_loaded)
        self.show_drive(self._root_path)
        QTimer.singleShot(500, self._report_top)

    # ---------- the root ----------

    def show_drive(self, path: str) -> None:
        """Root the model and the view at one folder or drive.

        `setRootPath()` and nothing else: it makes the path the model's root node,
        so the drive list is never built. `index(path)` looks equivalent and is not -
        it resolves the path through the root node, which is the drive list.
        """
        if not path:
            return
        self._root_path = path
        index = self._model.setRootPath(path)
        # isinstance, not just isValid: a stub that returns None must not reach Qt
        if isinstance(index, QModelIndex):
            self.setRootIndex(index)
        self.setCurrentIndex(QModelIndex())
        logger.info("show_drive: %s (valid=%s)", path, index.isValid() if index else False)

    def show_filesystem(self) -> None:
        """Show the panel's root again and drop the selection.

        Named for what it used to do. The filesystem top is no longer a place this
        panel goes: the drive list costs ~5.5 s per dead mapped drive on the GUI
        thread, so the drives are the window's selector and the panel always shows
        exactly one folder.
        """
        self._restore_top(force=True)
        self.setCurrentIndex(QModelIndex())
        logger.info(
            "show_filesystem: root=%r, view root valid=%s, rows=%d",
            self._model.rootPath(),
            self.rootIndex().isValid(),
            self._model.rowCount(self.rootIndex()),
        )

    def refresh(self) -> None:
        """Re-read the current root from scratch.

        Only the explicit `refresh` command asks for this: it costs a directory
        listing with a stat per entry, on the GUI thread, so nothing that touches the
        database may trigger it.

        The re-entrancy guard is load-bearing. A nested event loop can deliver an
        event while a listing is already in progress, and a second listing started on
        top of the first is how the queue stops draining.
        """
        if self._refreshing:
            logger.debug("refresh already in progress, not starting another")
            return
        self._refreshing = True
        try:
            self._restore_top(force=True)
        finally:
            self._refreshing = False

    def _restore_top(self, force: bool = False) -> None:
        """Put the model and the view back at the panel's root.

        `force` steps through the parent first: setRootPath() with the path it
        already has returns early, and that early return is the only thing stopping
        a real re-read.
        """
        if force:
            parent = str(Path(self._root_path).parent)
            if parent == self._root_path:  # a drive root: nothing above it
                parent = QDir.homePath()
            self._model.setRootPath(parent)
        index = self._model.setRootPath(self._root_path)
        # isinstance, not just isValid: a stub that returns None must not reach Qt
        if isinstance(index, QModelIndex):
            self.setRootIndex(index)

    def reveal(self, path: Path) -> bool:
        """Show `path`: select it and scroll it into view, then return to the root.

        Deliberately not index(path) alone: that one fetches with fetch=false and
        hands back an invalid index for a folder that is cached but filtered out - a
        hidden one such as AppData. So index() is tried first and setRootPath() is
        the fallback for a path the model has not listed yet.

        A path that is not on disk is not revealed. The check is on the path itself,
        not on fileInfo(index): setRootPath() on a missing path hands back the node of
        the nearest existing ancestor, so fileInfo(index).exists() answers about the
        parent and says True.
        """
        target = str(Path(path))
        started = time.perf_counter()

        index = self._model.index(target)
        rerooted = not index.isValid()
        if rerooted:
            if not Path(target).exists():
                return False
            index = self._model.setRootPath(target)  # never listed: this lists it

        if not isinstance(index, QModelIndex) or not index.isValid():
            if rerooted:
                self._restore_top()
            return False

        self._expand_ancestors(index)
        self.setCurrentIndex(index)
        self.scrollTo(index, QAbstractItemView.ScrollHint.PositionAtCenter)
        self._restore_top()

        logger.info(
            "revealed %s in %.0f ms%s; root=%r, view root valid=%s, rows=%d",
            target,
            (time.perf_counter() - started) * 1000,
            " (listed)" if rerooted else "",
            self._model.rootPath(),
            self.rootIndex().isValid(),
            self._model.rowCount(self.rootIndex()),
        )
        return True

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

    # ---------- logging and paths ----------

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
                "filesystem panel: %d entries at the root", self._model.rowCount(self.rootIndex())
            )
        except RuntimeError:
            return

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
