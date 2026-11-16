"""The window's shape: panels, grid, preview, toolbar, menus.

Nothing here decides what a command does - it only decides where it lives. The
actions are window actions, not toolbar actions, so a command whose button ends up in
the overflow menu is still reachable by its shortcut, and the palette is for discovery.
"""

import logging
from functools import partial
from pathlib import Path

from PySide6.QtCore import QSize, Qt, QTimer
from PySide6.QtGui import QAction
from PySide6.QtWidgets import (
    QComboBox,
    QGroupBox,
    QLabel,
    QListWidget,
    QSplitter,
    QStatusBar,
    QTabWidget,
    QToolBar,
    QVBoxLayout,
)

from meret.core.debug import enabled
from meret.core.locale import _
from meret.gui import icons
from meret.gui.commands import SHORTCUTS, command_table, menu_table, short_label
from meret.gui.drives import drive_roots
from meret.gui.fs_panel import RealFsPanel
from meret.gui.tag_tree import TagTree
from meret.gui.viewer import ViewerView
from meret.gui.window.constants import ICON, TOOLBAR

logger = logging.getLogger(__name__)


class LayoutMixin:
    """`_build_ui` and the three builders that hang off it."""

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

        Buttons with text do not fit a 1280px window: Qt would push the tail into a
        ">>" menu. Icons only, with the description and the key in the tooltip.
        Switch to ToolButtonTextBesideIcon for words instead.
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
