"""The command table and its palette, one to one with the TUI.

Every action of `MeretApp.BINDINGS` is here, every entry of the TUI's palette, and
`untag` - which the TUI lists but never wires up. Same commands, same descriptions,
and `tests/gui/test_commands.py` compares the two tables, so they cannot drift
apart. The table is a function, not a constant, so the strings are translated on
call rather than at import time.
"""

import logging

from PySide6.QtCore import QEvent, Qt
from PySide6.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QVBoxLayout,
)

from meret.core.locale import _

logger = logging.getLogger(__name__)

# Qt keys, the counterpart of the TUI's bindings: the TUI binds every action, so a
# command without a key here would be reachable from the palette alone.
SHORTCUTS: dict[str, str] = {
    "refresh": "F5",
    "scan": "F6",
    "search": "Ctrl+F",
    "tag": "Ctrl+T",
    "untag": "Ctrl+Shift+T",
    "delete": "Ctrl+D",
    "sort_files": "Alt+S",
    "init_db": "Ctrl+I",
    "config": "Ctrl+,",
    "stats": "Alt+I",
    "tree": "Alt+L",
    "prune": "Alt+P",
    "mount": "Alt+M",
    "umount": "Alt+U",
    "command_palette": ":",
    "help": "F1",
    "quit": "Ctrl+Q",
}


def command_table() -> list[tuple[str, str]]:
    """(command, description) - the TUI's bindings and palette, verbatim.

    Minus home / up / select_drive / open_file: the real-FS panel is one tree from
    the filesystem top, and a file opens on a double click, so the GUI has nothing
    left to navigate to and nothing left to hand over to the system.
    """
    return [
        ("refresh", _("Refresh")),
        ("scan", _("Scan directory for media")),
        ("search", _("Search files by tags/name")),
        ("tag", _("Tag selected file")),
        ("untag", _("Remove tag from file")),
        ("delete", _("Delete tag")),
        ("sort_files", _("Cycle sort: name / size / date")),
        ("init_db", _("Initialize new database")),
        ("config", _("Edit configuration")),
        ("stats", _("Show database statistics")),
        ("tree", _("Show tag tree")),
        ("prune", _("Remove orphaned tags")),
        ("mount", _("Mount virtual filesystem")),
        ("umount", _("Unmount filesystem")),
        ("command_palette", _("Command palette")),
        ("help", _("Help")),
        ("quit", _("Quit")),
    ]

def menu_table() -> list[tuple[str, tuple[str | None, ...]]]:
    """(menu, commands) - the menu bar: the same commands as the toolbar, grouped.

    A function, not a constant, for the same reason as command_table(): the menu
    titles have to be translated on call, not at import time. None is a separator.
    """
    return [
        (
            _("File"),
            (
                "refresh",
                "scan",
                "search",
                None,
                "init_db",
                "config",
                None,
                "mount",
                "umount",
                None,
                "quit",
            ),
        ),
        (_("Edit"), ("tag", "untag", "delete")),
        (_("View"), ("sort_files", "stats", "tree", "prune")),
        (_("Help"), ("command_palette", "help")),
    ]

def short_label(command: str) -> str:
    """Text for a toolbar button. Falls back to the full description."""
    short = {
        "scan": _("Scan"),
        "search": _("Search"),
        "tag": _("Tag"),
    }
    return short.get(command) or dict(command_table()).get(command, command)

def matches(query: str) -> list[str]:
    """Commands for the typed text: exact, then name prefix, then anywhere."""
    query = query.strip().lower()
    hits = [
        command
        for command, description in command_table()
        if query in command or query in description.lower()
    ]
    return sorted(
        hits, key=lambda command: (command != query, not command.startswith(query))
    )

def _column_widths() -> tuple[int, int]:
    """Palette column widths, taken from the data instead of hardcoded.

    A fixed width glues the columns the moment a key or a name outgrows it:
    "Ctrl+Shift+T" is 13 characters and "command_palette" is 15, so a hardcoded
    11/15 leaves no space before the description.
    """
    keys = max((len(key) for key in SHORTCUTS.values()), default=1)
    names = max((len(command) for command, _ in command_table()), default=1)
    return keys + 2, names + 2


class CommandPalette(QDialog):
    """Type-to-filter command list, opened with ':' exactly like in the TUI.

    Every entry shows its key beside the name, so the palette is also the help the
    TUI keeps in its footer.
    """

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle(_("Command palette"))
        self.setMinimumWidth(480)

        self._input = QLineEdit(self)
        self._input.setPlaceholderText(_("Command..."))
        self._input.installEventFilter(self)

        self._list = QListWidget(self)
        self._list.setAlternatingRowColors(True)

        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel,
            parent=self,
        )
        buttons.accepted.connect(self._accept_best)
        buttons.rejected.connect(self.reject)

        layout = QVBoxLayout(self)
        layout.addWidget(self._input)
        layout.addWidget(self._list)
        layout.addWidget(buttons)

        self._input.textChanged.connect(self._refill)
        self._input.returnPressed.connect(self._accept_best)
        self._list.itemActivated.connect(self._accept_item)
        self._list.itemDoubleClicked.connect(self._accept_item)
        self._refill("")
        self._fit()
        self._input.setFocus()

    def _fit(self) -> None:
        """Grow until every command is visible at once.

        A palette you have to scroll looks like a palette with missing commands,
        and the whole point of it is to show what exists.
        """
        rows = max(self._list.count(), 1)
        row = self._list.sizeHintForRow(0) or 22
        self._list.setMinimumHeight(rows * row + 4)
        self.resize(560, min(780, rows * row + self._input.sizeHint().height() + 96))

    # ---------- filling ----------

    def _refill(self, query: str) -> None:
        """Rebuild the list for the typed text and keep the first row current."""
        descriptions = dict(command_table())
        key_width, name_width = _column_widths()
        self._list.clear()
        for command in matches(query):
            key = SHORTCUTS.get(command) or "—"
            item = QListWidgetItem(
                f"{key:<{key_width}}{command:<{name_width}}{descriptions[command]}"
            )
            item.setData(Qt.ItemDataRole.UserRole, command)
            self._list.addItem(item)
        self._list.setCurrentRow(0 if self._list.count() else -1)

    def _move(self, step: int) -> None:
        count = self._list.count()
        if not count:
            return
        row = min(max(self._list.currentRow() + step, 0), count - 1)
        self._list.setCurrentRow(row)

    # ---------- choosing ----------

    def chosen(self) -> str | None:
        """The command to run, or None when the palette was dismissed."""
        item = self._list.currentItem()
        return item.data(Qt.ItemDataRole.UserRole) if item is not None else None

    def _accept_best(self) -> None:
        """Enter takes the best match even with the caret still in the input."""
        best = matches(self._input.text())
        if not best:
            self.reject()
            return
        self._select(best[0])

    def _accept_item(self, item: QListWidgetItem) -> None:
        self._select(item.data(Qt.ItemDataRole.UserRole))

    def _select(self, command: str) -> None:
        for row in range(self._list.count()):
            if self._list.item(row).data(Qt.ItemDataRole.UserRole) == command:
                self._list.setCurrentRow(row)
                break
        self.accept()

    # ---------- keys ----------

    def eventFilter(self, watched, event) -> bool:
        """Up/Down move the list while the input keeps the focus, as in the TUI."""
        if watched is self._input and event.type() == QEvent.Type.KeyPress:
            key = event.key()
            if key == Qt.Key.Key_Down:
                self._move(1)
                return True
            if key == Qt.Key.Key_Up:
                self._move(-1)
                return True
        return super().eventFilter(watched, event)