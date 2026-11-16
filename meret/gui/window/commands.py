"""The command handlers: what each palette row and toolbar button actually does.

Every handler reports through `_say()`, never `_status_label.setText()`. The difference
is not cosmetic: `_update_status()` runs after almost every action, so a message written
with setText() survives about one poll - and the messages here ("Select a real file
first!", "Scan complete!") are exactly the ones read when nothing else is happening.
"""

import threading
import logging
import time
from functools import partial

from PySide6.QtCore import QSize, Qt, QTimer
from PySide6.QtWidgets import QApplication, QDialog, QInputDialog, QMessageBox

from meret.core import tagpaths
from meret.core.autotag import AutoTagger
from meret.core.config import get_config
from meret.core.debug import enabled
from meret.core.indexer import Indexer
from meret.core.locale import _
from meret.gui import icons
from meret.gui.commands import SHORTCUTS, CommandPalette, command_table
from meret.gui.settings import SettingsDialog, apply_settings
from meret.gui.tag_tree import TagTree
from meret.gui.theme import apply_theme
from meret.gui.thumbnails import ThumbnailCache
from meret.gui.window.constants import ICON, NAVIGATION_COMMANDS
from meret.gui.window.progress import ProgressDialog

logger = logging.getLogger(__name__)


class CommandsMixin:
    """The command table, its handlers, the settings dialog and the help."""

    # ---------- dispatch ----------

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
        self._notice = None  # a new action replaces whatever was on the line
        handlers = {
            # the one command that re-reads the disk: see _refresh's `panel`
            "refresh": partial(self._refresh, panel=True),
            "scan": self._scan,
            "search": self._search_query,
            "tag": self._tag_selected,
            "untag": self._untag_selected,
            "autotag": self._autotag,
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
            self._say(_("'{cmd}' isn't available in the GUI yet").format(cmd=cmd))
            return
        handler()

    # ---------- tags ----------

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
            self._say(_("Select a real file first!"))
            return

        name, accepted = QInputDialog.getText(self, _("Tag"), _("Enter tag (e.g., vacation/beach)"))
        if not accepted or not name.strip():
            return
        for file_id, _path in targets:
            self._repo.add_tag(file_id, name.strip())
        self._refresh()

    def _untag_selected(self) -> None:
        """Remove a tag from the selected files."""
        targets = self._target_files()
        if not targets:
            self._say(_("Select a real file first!"))
            return

        name, accepted = QInputDialog.getText(self, _("Remove tag from file"), _("Tag path"))
        if not accepted or not name.strip():
            return
        for file_id, _path in targets:
            self._repo.remove_tag(file_id, name.strip())
        self._refresh()

    def _autotag(self) -> None:
        """Apply the auto-tagging rules to the whole library, with a progress window.

        Cancel is a `threading.Event` owned here and asked by the tagger before each
        file, so the library is left with every processed file fully tagged.
        """
        tagger = AutoTagger.from_config()
        if not tagger.rules:
            self._say(_("No auto-tagging rules: set autotag.rules_file or pass --rules on the CLI"))
            return

        self._poll.stop()
        dialog = ProgressDialog(_("Tagging..."), self)
        stop = threading.Event()
        dialog.cancelled.connect(stop.set)
        dialog.show()

        def report(done: int, total: int) -> None:
            """Pool thread: hand the step to the GUI thread through the signal."""
            dialog.report("", done, total)

        def done(stats: dict) -> None:
            dialog.finish()
            if stats.get("interrupted"):
                self._say(_("Tagging cancelled"))
            else:
                self._say(
                    _("Tagged: {files} files, {tags} tags").format(
                        files=stats["changed"], tags=stats["tags"]
                    )
                )
            # the run itself was the change: re-baseline, so the first poll after it
            # does not refresh a second time on data the refresh below already read
            self._version = None
            self._refresh()
            self._poll.start()

        def failed(error: Exception) -> None:
            dialog.finish()
            logger.warning("autotag failed: %s", error)
            self._say(_("Error: {error}").format(error=error))
            self._version = None
            self._poll.start()

        self._tasks.run(
            tagger.run,
            on_done=done,
            on_failed=failed,
            args=(self._repo,),
            kwargs={"progress": report, "should_stop": stop.is_set},
            sticky=True,
        )

    def _delete_tag(self) -> None:
        """Delete the tag selected in the tree. Its children move up one level."""
        item = self.tags.currentItem()
        if item is None or item.data(0, Qt.ItemDataRole.UserRole) == "/":
            self._say(_("Select a tag folder in the virtual panel first"))
            return

        # The tree shows sanitized names; the repository needs the raw tag path
        tag_id = item.data(0, TagTree.ROLE_TAG_ID)
        by_id = {tag.id: tag for tag in self._repo.get_all_tags()}
        tag = by_id.get(tag_id)
        if tag is None:
            self._say(_("No such path: {path}").format(path=item.text(0)))
            return

        path = tagpaths.path_of(tag, by_id)
        if not self._repo.delete_tag(path):
            self._say(_("No such path: {path}").format(path=path))
            return
        self._say(_("Deleted tag {path}").format(path="/" + path))
        self._refresh()

    def _show_tree(self) -> None:
        """The tag tree is the virtual panel: reload it and move focus there."""
        self.tags.reload()
        self.tags.setFocus()

    def _prune(self) -> None:
        """Delete tags with no files and no subtags."""
        self._tasks.run(self._repo.prune_orphan_tags, on_done=self._on_pruned)

    def _on_pruned(self, removed: int) -> None:
        self._say(_("Tags deleted: {n}").format(n=removed))
        self._refresh()

    # ---------- scan and search ----------

    def _scan(self) -> None:
        """Index the folder picked in the real filesystem panel, with a progress window.

        The poll is stopped for the duration: the scan commits the whole time, and a
        refresh every 1.5 s would rebuild the tree and the grid underneath it. The task
        is sticky, so browsing during a long scan cannot swallow its end.

        Cancel is the indexer's own flag, not a killed thread: what was written stays
        written, and the run stops on a file boundary.
        """
        target = self.fs.current_path()
        if target is None:
            self._say(_("Select a real file first!"))
            return

        folder = target if target.is_dir() else target.parent
        self._poll.stop()
        dialog = ProgressDialog(_("Scanning {path}").format(path=folder), self)
        indexer = Indexer(self._repo, folder)
        dialog.cancelled.connect(indexer.cancel)
        dialog.show()

        def done(stats: dict) -> None:
            dialog.finish()
            self._on_scan_done(stats)

        def failed(error: Exception) -> None:
            dialog.finish()
            self._on_scan_failed(error)

        self._tasks.run(
            indexer.run,
            on_done=done,
            on_failed=failed,
            kwargs={"progress_callback": dialog.report},
            sticky=True,
        )

    def _on_scan_done(self, stats: dict) -> None:
        if stats.get("interrupted"):
            self._say(_("Scan cancelled"))
        else:
            self._say(_("Scan complete!"))
        # The scan itself was the change: re-baseline, so the first poll after it
        # does not refresh a second time on data the refresh below already read
        self._version = None
        self._refresh()
        self._poll.start()

    def _on_scan_failed(self, error: Exception) -> None:
        logger.warning("scan failed: %s", error)
        self._say(_("Error: {error}").format(error=error))
        self._version = None
        self._poll.start()

    def _search_query(self) -> None:
        """Ask for a query: a name, or `tag:travel` - the TUI's own rule."""
        query, accepted = QInputDialog.getText(self, _("Search"), _("Text in the file name"))
        if not accepted or not query.strip():
            return
        self._run_search(query.strip())

    # ---------- statistics, sorting, help ----------

    def _stats(self) -> None:
        """Database statistics, read off the event loop."""
        self._tasks.run(self._repo.get_stats, on_done=self._on_stats)

    def _on_stats(self, stats: dict) -> None:
        self._say(
            _("Files: {files}, tags: {tags}").format(
                files=stats.get("total_files", 0), tags=stats.get("total_tags", 0)
            )
        )

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
            Qt.SortOrder.AscendingOrder if self._sort_index == 0 else Qt.SortOrder.DescendingOrder
        )
        self.fs.sortByColumn(column, order)
        self._say(_("Sort by: {mode}").format(mode=modes[self._sort_index]))

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
            line(name) for name, _description in command_table() if name not in NAVIGATION_COMMANDS
        )
        QMessageBox.information(self, _("Help"), "\n".join(lines))

    # ---------- settings ----------

    def _edit_config(self) -> None:
        """Open the settings and apply the result when the window closes.

        Deliberately not exec(): that runs a nested event loop, and the poll, the
        thread pool and the dialog end up fighting over the same window.

        The raise_/activateWindow pair is deferred, not immediate. open() already
        raises and activates, but on Windows the owner can take the foreground back
        in that same turn: the dialog ends up behind a window that modality has
        already blocked, and the app looks dead, because Qt drops every click on
        that window. --debug settings_modeless removes native modality altogether.

        One copy of this body, not two: the duplicate that used to sit below the
        return connected `finished` twice, so every save ran its callback twice and
        the poll was restarted by the second run.
        """
        manager = get_config()
        dialog = SettingsDialog(self)

        def finished(result: int) -> None:
            """The dialog is gone: let the poll run again and apply what was chosen."""
            self._paused = False
            self._pump(self._tasks.generation)  # the pause cut the chain in half
            self._poll.start()
            if result != QDialog.DialogCode.Accepted:
                self._say(_("Settings unchanged"))
            else:
                apply_settings(dialog.values())
                self._apply_look()
                self._say(_("Settings saved to {path}").format(path=manager.config_path))
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
        if enabled("settings_modeless"):
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
            apply_theme(QApplication.instance(), gui.theme)
            self._theme = gui.theme
            # The glyphs are painted in the palette colour: re-tint them with it
            icons.clear_cache()
            for command, action in self._actions.items():
                action.setIcon(icons.command_icon(command, ICON))
            self.viewer.retint()

        if gui.thumbnail_size != self._thumbs.size:
            self.grid.setIconSize(QSize(gui.thumbnail_size, gui.thumbnail_size))
            self._thumbs = ThumbnailCache(size=gui.thumbnail_size, scope=str(self._db_path))
