"""The Qt front-end: window, panels and the event loop.

Qt objects live on the GUI thread only. Anything that touches SQLite or decodes
an image goes through TaskRunner and comes back as a signal. The tag panel reads
the same VirtualTree a mount would serve, so the two can never disagree, and the
real filesystem panel is plain Qt - it only picks a path to scan or to tag.

Mounting and unmounting shell out to `meret mount` / `meret umount` instead of
importing a backend, so opening a window never drags FUSE or WinFsp in.
"""

import faulthandler
import logging
import sys
import time
from functools import partial
from pathlib import Path

from PySide6.QtCore import QSize, Qt, QTimer
from PySide6.QtGui import QAction
from PySide6.QtWidgets import (
    QApplication,
    QComboBox,
    QDialog,
    QGroupBox,
    QInputDialog,
    QLabel,
    QListWidget,
    QMessageBox,
    QSplitter,
    QStatusBar,
    QTabWidget,
    QToolBar,
    QVBoxLayout,
    QWidget,
)

from meret.core import tagpaths
from meret.core.autotag import AutoTagger
from meret.core.config import get_config
from meret.core.debug import enabled
from meret.core.indexer import Indexer
from meret.core.locale import _
from meret.core.sqlite_repo import SQLiteRepository
from meret.gui import icons
from meret.gui.commands import SHORTCUTS, CommandPalette, command_table, menu_table, short_label
from meret.gui.diagnostics import TracedApplication, quiet_media_logs
from meret.gui.drives import drive_roots
from meret.gui.fs_panel import RealFsPanel
from meret.gui.settings import SettingsDialog, apply_settings
from meret.gui.tag_tree import TagTree
from meret.gui.theme import apply_theme
from meret.gui.thumbnails import ThumbnailCache
from meret.gui.viewer import ViewerView
from meret.gui.window.base import WindowBase
from meret.gui.window.contents import ContentsMixin
from meret.gui.window.databases import DatabaseMixin
from meret.gui.window.mounts import MountMixin
from meret.gui.window.polling import PollMixin

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
#: Toolbar buttons, in order. None is a separator. This is every command: a panel
#: that hides half of them is not "one to one with the TUI".
TOOLBAR: tuple = (
    "refresh",
    "scan",
    "search",
    None,
    "tag",
    "untag",
    "autotag",
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


class MainWindow(WindowBase, MountMixin, PollMixin, DatabaseMixin, ContentsMixin):
    """Browse the tag tree, the files under one tag, and a preview.

    The state and the lifetime are WindowBase's, the shell-outs MountMixin's, the poll
    PollMixin's, the database switch DatabaseMixin's and the browser ContentsMixin's;
    what is left here is the layout and the commands, which are the last two mixins.
    """

    def __init__(self, repo, db_path, parent: QWidget | None = None) -> None:
        super().__init__(repo, db_path, parent)

    def _build_ui(self) -> None:
        splitter = QSplitter(Qt.Orientation.Horizontal, self)

        self.tags = TagTree(self._tree, self._tasks, self)
        self.tags.setMinimumWidth(180)
        self.tags.tag_selected.connect(self.open_folder)

        self.fs = RealFsPanel(self)
        self.fs.path_activated.connect(self._on_real_activated)
        # The database folder is not revealed at start-up: reveal() expands every
        # ancestor, and each expand is a listing with a stat per entry on the GUI
        # thread. The panel starts at the home folder, and the drive selector below
        # takes the user anywhere else.
        logger.info("database folder: %s", self._db_path.parent)

        left = QSplitter(Qt.Orientation.Vertical, self)
        tags_box = QGroupBox(_("Virtual FS (Tags)"), self)
        QVBoxLayout(tags_box).addWidget(self.tags)

        fs_box = QGroupBox(_("Real Filesystem"), self)
        fs_layout = QVBoxLayout(fs_box)
        # Letters from a bitmask, not from QDir.drives(): that one queries every
        # volume, and a dead mapped drive faults inside the provider.
        self.drives = QComboBox(self)
        self.drives.addItem(str(Path.home()))
        for root in drive_roots():
            self.drives.addItem(root)
        self.drives.currentTextChanged.connect(self.fs.show_drive)
        fs_layout.addWidget(self.drives)
        fs_layout.addWidget(self.fs)

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
        self.grid.setMinimumWidth(240)
        self.grid.setIconSize(QSize(self._thumbs.size, self._thumbs.size))
        self.grid.itemSelectionChanged.connect(self._show_preview)
        self.grid.itemDoubleClicked.connect(self._open_in_viewer)

        splitter.addWidget(self.grid)
        splitter.addWidget(self.preview)
        # Load-bearing. The splitter is resized twice - once when it becomes a page
        # of the tab widget, once with the window - and with every stretch factor at
        # zero the extra width all goes to the left pane: the grid and the preview
        # collapse to nothing and the browser looks empty.
        splitter.setStretchFactor(0, 0)
        splitter.setStretchFactor(1, 3)
        splitter.setStretchFactor(2, 2)
        splitter.setSizes([280, 720, 420])

        # The browser (panels + grid + preview) is one tab, the viewer another
        self.tabs = QTabWidget(self)
        self.tabs.addTab(splitter, _("Browser"))
        self.viewer = ViewerView(self)
        # Stepping is the window's: the viewer has no idea what a folder is
        self.viewer.next_requested.connect(self._next_file)
        self.viewer.previous_requested.connect(self._previous_file)
        self.tabs.addTab(self.viewer, _("Viewer"))
        # Leaving the viewer tab stops playback, so a hidden video keeps silent
        self.tabs.currentChanged.connect(self._on_tab_changed)
        self.setCentralWidget(self.tabs)

        # The page gets its real width only after the first layout pass: ask once
        # more, or the splitter keeps whatever sizes it was built with
        QTimer.singleShot(0, partial(self._apply_split, splitter))

        self._status_label = QLabel(_("No selection"), self)
        bar = QStatusBar(self)
        bar.addPermanentWidget(self._status_label)
        self.setStatusBar(bar)

    @staticmethod
    def _apply_split(splitter: QSplitter) -> None:
        """Re-assert the pane widths once the page has its final size.

        A singleShot outlives the widget it was started from, so the C++ object
        behind the splitter can already be gone by the time this runs.
        """
        try:
            splitter.setSizes([280, 720, 420])
        except RuntimeError:
            return

    def _build_actions(self) -> None:
        """One QAction per command, with its key and its glyph.

        They are window actions, not toolbar actions, so a command whose button is
        in the overflow menu is still reachable by its shortcut - and the palette
        is for discovery.

        --debug no_tooltips drops the tooltips for an A/B run. A QToolTip is a
        top-level QLabel, and it is the one widget that shows up in the watchdog
        report right before a window stops answering: if a freeze goes away without
        it, the tooltip path is the bug.
        """
        tooltips = not enabled("no_tooltips")
        for command, description in command_table():
            shortcut = SHORTCUTS.get(command, "")
            action = QAction(description, self)
            action.setIcon(icons.command_icon(command, ICON))
            action.setIconText(short_label(command))  # the button text, if shown
            if shortcut:
                action.setShortcut(shortcut)
                if tooltips:
                    action.setToolTip(f"{description} ({shortcut})")
            elif tooltips:
                action.setToolTip(description)
            action.triggered.connect(lambda _checked=False, name=command: self.run_command(name))
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

        One loop, not two. A duplicated body adds a second set of menus to the same
        bar: the tests see eight top-level menus instead of four and twice as many
        entries, which looks like a wiring bug and is a paste.
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
            self._status_label.setText(_("'{cmd}' isn't available in the GUI yet").format(cmd=cmd))
            return
        handler()

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
            self._status_label.setText(_("Select a real file first!"))
            return

        name, accepted = QInputDialog.getText(self, _("Remove tag from file"), _("Tag path"))
        if not accepted or not name.strip():
            return
        for file_id, _path in targets:
            self._repo.remove_tag(file_id, name.strip())
        self._refresh()

    def _autotag(self) -> None:
        """Apply the auto-tagging rules to the whole library, off the GUI thread.

        The rules live in a file, so this is the same work `meret autotag` does - the
        window only decides when. No `only_untagged` override and no `dry_run`: the
        config decides, exactly as it does for the CLI, so one file drives both.

        The poll is stopped for the duration, like a scan: the run commits the whole
        time, and a refresh every 1.5 s would rebuild the tree and the grid underneath
        it. The task is sticky, so browsing during a long run cannot swallow its end.
        """
        tagger = AutoTagger.from_config()
        if not tagger.rules:
            self._status_label.setText(
                _("No auto-tagging rules: set autotag.rules_file or pass --rules on the CLI")
            )
            return

        self._status_label.setText(_("Tagging..."))
        self._poll.stop()

        def done(stats: dict) -> None:
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
            logger.warning("autotag failed: %s", error)
            self._say(_("Error: {error}").format(error=error))
            self._version = None
            self._poll.start()

        self._tasks.run(tagger.run, on_done=done, on_failed=failed, args=(self._repo,), sticky=True)

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

    def _stats(self) -> None:
        """Database statistics, read off the event loop."""
        self._tasks.run(self._repo.get_stats, on_done=self._on_stats)

    def _on_stats(self, stats: dict) -> None:
        self._say(
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
        self._say(_("Tags deleted: {n}").format(n=removed))
        self._refresh()

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
        self._status_label.setText(_("Sort by: {mode}").format(mode=modes[self._sort_index]))

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


def run_gui(db_path: Path, argv: list[str] | None = None) -> int:
    """Open the database, show the window, run the event loop. Returns the exit code.

    With --debug watchdog the run is traced: every thread's Python stack is dumped
    every three seconds, and the application counts and times every event. A freeze
    the suite cannot reproduce leaves no other trace, and "the loop stopped" and
    "the loop is spinning" need opposite fixes - the trace tells them apart.
    """
    quiet_media_logs()
    watch = enabled("watchdog")
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
