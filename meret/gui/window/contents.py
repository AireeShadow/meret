"""The browser: the grid, the preview, the thumbnail batches and the search.

Everything here reads the virtual tree and paints what it finds. Anything that can
block - a listing, a decode - goes through TaskRunner and comes back as a signal
carrying a generation, so a result that arrives late cannot paint over a newer one.
"""

import logging
from functools import partial
from pathlib import Path

from PySide6.QtCore import QTimer, QUrl
from PySide6.QtGui import QDesktopServices, QIcon, QPixmap
from PySide6.QtWidgets import QListWidgetItem, QStyle

from meret.core import tagpaths
from meret.core.locale import _
from meret.core.models import FileEntry, FileType
from meret.gui import icons
from meret.gui.thumbnails import Reason, Thumbnail
from meret.vfs.tree import Node, VirtualTree

logger = logging.getLogger(__name__)

#: Thumbnails requested per round - a huge folder must not flood the pool queue.
BATCH = 24
#: Preview size, in pixels.
PREVIEW = 512


def _reason_text(reason: str) -> str:
    """Translated tooltip for a missing preview. Resolved at call time."""
    table = {
        Reason.NO_TOOL: _("No preview: ffmpeg is not installed"),
        Reason.NO_FRAME: _("No preview: no frame could be read"),
        Reason.UNREADABLE: _("No preview: the file could not be read"),
        Reason.NO_DECODER: _("No preview for this file type"),
    }
    return table.get(reason, "")


class ContentsMixin:
    """The browser: listings, thumbnails, the preview and the status line."""

    # ---------- listings ----------

    def _refresh(self, panel: bool = False) -> None:
        """Re-read tags and the current view after a write.

        `panel` re-reads the real filesystem panel too, and only the explicit refresh
        command passes it. The panel shows the disk, not the database: making a tag
        write re-list two directories on the GUI thread is what turned an outside
        change into a five-second freeze.
        """
        # None only while a database switch is in the air, and a switch always ends
        # in open_folder()
        assert self._tree is not None
        self._tree.invalidate()
        self.tags.reload()
        if panel:
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
        assert self._tree is not None
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

        def done(nodes: list[Node]) -> None:
            self._show_nodes(nodes, generation)

        def failed(error: Exception) -> None:
            self._show_error(self._vpath, error)

        self._tasks.run(
            self._tree.listdir,
            on_done=done,
            on_failed=failed,
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
                QTimer.singleShot(0, partial(self._pump, generation))
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

        def failed(item: QListWidgetItem, error: Exception) -> None:
            """A decoder that raised is not a crash: keep the placeholder, count it."""
            logger.warning("thumbnail failed for %s: %s", item.text(), error)
            landed(item, None)

        for file_id, source, mtime, item in todo:
            self._tasks.run(
                self._thumbs.load_result,
                on_done=partial(landed, item),
                on_failed=partial(failed, item),
                args=(Path(source),),
                kwargs={"file_id": file_id, "mtime": mtime},
                generation=generation,
            )

    # ---------- icons and tooltips ----------

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
        if result.ok and result.image is not None:
            item.setIcon(QIcon(QPixmap.fromImage(result.image)))
            item.setToolTip(self._file_tooltip(item))
            return
        item.setToolTip(self._file_tooltip(item, _reason_text(result.reason)))

    # ---------- the status line ----------

    def _update_status(self) -> None:
        """Refresh the status bar: current view, selection count, or a notice.

        A notice wins. This runs after almost every action - a listing, a selection, a
        refresh - so anything written with setText() alone survives about one poll.
        """
        if self._search is not None:
            where = _("Search: {query}").format(query=self._search)
        else:
            where = self._vpath
        self.statusBar().showMessage(f"{where} — {self.grid.count()}")
        if self._notice is not None:
            self._status_label.setText(self._notice)
        elif self._selection:
            self._status_label.setText(f"{_('Selected:')} {len(self._selection)}")
        else:
            self._status_label.setText(_("No selection"))

    def _say(self, text: str) -> None:
        """Show a message and keep it until the user asks for something else.

        Written through `_notice` so the next `_update_status()` cannot wipe it. That
        is what happened to "Error: database is locked": the poll's refresh replaced
        it with "No selection" about a second and a half later, so the message was
        technically shown and practically never read.
        """
        self._notice = text
        self._status_label.setText(text)

    def _show_error(self, path: str, error: Exception) -> None:
        """A listing that failed must be visible, never a silently empty grid."""
        logger.warning("cannot list %s: %s", path, error)
        self._say(_("Cannot open {path}: {error}").format(path=path, error=error))

    # ---------- search ----------

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

            def work() -> list[FileEntry]:
                return self._repo.search_by_tags([tag])
        else:

            def work() -> list[FileEntry]:
                return self._repo.search_by_name(query)

        generation = self._tasks.bump()

        def done(files: list[FileEntry]) -> None:
            self._show_files(files, generation)

        def failed(error: Exception) -> None:
            self._show_error(query, error)

        self._tasks.run(work, on_done=done, on_failed=failed, generation=generation)

    def _show_files(self, files: list[FileEntry], generation: int) -> None:
        """Fill the grid from search results instead of a folder listing."""
        if generation != self._tasks.generation:
            return
        for entry in files:
            mtime = self._entry_mtime(entry)
            item = self._add_file_item(entry.filename, entry.filepath, entry.id, mtime, entry.type)
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

    # ---------- preview ----------

    def _show_preview(self) -> None:
        selected = self.grid.selectedItems()
        self._selection = {
            key
            for key in (
                item.data(self.ROLE_VPATH) or item.data(self.ROLE_SOURCE) for item in selected
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

        def done(result: Thumbnail) -> None:
            self._set_preview(result, item.text(), token)

        self._tasks.run(
            self._thumbs.load_result,
            on_done=done,
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
        if not result.ok or result.image is None:
            self.preview.setPixmap(QPixmap())
            reason = _reason_text(result.reason)
            self.preview.setText(f"{name}\n{reason}" if reason else name)
            return
        self.preview.setPixmap(QPixmap.fromImage(result.image))

    # ---------- opening ----------

    def _open_external(self, item: QListWidgetItem) -> None:
        """Open a file with the system handler, or descend into a folder."""
        source = item.data(self.ROLE_SOURCE)
        if source:
            QDesktopServices.openUrl(QUrl.fromLocalFile(source))
            return
        vpath = item.data(self.ROLE_VPATH)
        if vpath:
            self.open_folder(vpath)

    def _open_in_viewer(self, item: QListWidgetItem) -> None:
        """Double-click: open the file in the viewer tab, or descend into a folder."""
        source = item.data(self.ROLE_SOURCE)
        if not source:
            vpath = item.data(self.ROLE_VPATH)
            if vpath:
                self.open_folder(vpath)
            return

        self.tabs.setCurrentWidget(self.viewer)
        self.viewer.open_file(str(source))
        # The viewer's shortcuts are WidgetWithChildrenShortcut: without the focus
        # the arrow keys and Space do nothing at all
        self.viewer.setFocus()

    def _step_file(self, offset: int) -> None:
        """Open the file `offset` rows away from the current one in the grid.

        Folder rows are skipped: they have no source to show. Stepping past either
        end stops and says so - a wrap from the last file back to the first reads as
        a jump, not as an end.
        """
        if self.grid.count() == 0:
            return

        step = 1 if offset > 0 else -1
        index = self.grid.currentRow()
        if index < 0:
            index = -1 if step > 0 else self.grid.count()

        while True:
            index += step
            if index < 0 or index >= self.grid.count():
                self._say(_("End of the folder"))
                return
            item = self.grid.item(index)
            if item.data(self.ROLE_SOURCE):
                break

        self.grid.setCurrentRow(index)
        self.grid.scrollToItem(item)
        self._open_in_viewer(item)

    def _next_file(self) -> None:
        """Viewer 'next': one row down in the grid."""
        self._step_file(1)

    def _previous_file(self) -> None:
        """Viewer 'previous': one row up in the grid."""
        self._step_file(-1)

    def _on_tab_changed(self, index: int) -> None:
        """A hidden video must not keep playing: stop it when the tab goes away."""
        widget = self.tabs.widget(index)
        if widget is not self.viewer:
            self.viewer.stop()

    def _on_real_activated(self, path: str) -> None:
        """A real file: jump to its first tag folder, or hand it to the system."""
        target = Path(path)
        if target.is_dir():
            self._say(_("Path: {path}").format(path=target))
            return

        entry = self._repo.get_file_by_path(str(target))
        if entry is None:
            self._say(_("File is not in the database: {path}").format(path=target))
            return

        if entry.labels:
            known = tagpaths.name_to_path(self._repo.get_all_tags())
            first = known.get(entry.labels[0].casefold())
            if first:
                self.tags.reload()
                self.open_folder("/" + first)
                return
        QDesktopServices.openUrl(QUrl.fromLocalFile(str(target)))
