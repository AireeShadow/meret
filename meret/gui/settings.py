"""The settings dialog: every config field as a widget, one tab per section.

The file stays the source of truth - the dialog reads the manager and writes back
through it, and offers a button to open the file for hand edits. The widget for a
field follows the field's own kind, and a number's bounds come from the pydantic
model, so the dialog cannot offer a value the config would then reject.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFileDialog,
    QFormLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QSpinBox,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from meret.core.config import MeretConfig, get_config
from meret.core.locale import _


@dataclass(frozen=True)
class Setting:
    """One config field, and how it has to be shown."""

    section: str
    name: str
    label: str
    kind: str  # "int" | "bool" | "choice" | "path" | "text"
    choices: Tuple[str, ...] = ()

    @property
    def key(self) -> str:
        """The dot-key the config manager knows this field by."""
        return f"{self.section}.{self.name}"


def sections() -> Tuple[Tuple[str, str], ...]:
    """(section, title), in the order the tabs are shown."""
    return (
        ("paths", _("Paths")),
        ("scan", _("Scan")),
        ("gui", _("GUI")),
        ("locale", _("Locale")),
    )


def settings_table() -> List[Setting]:
    """Every field of the config. A function, so labels translate on call."""
    return [
        Setting("paths", "default_db", _("Default database"), "path"),
        Setting("paths", "default_library", _("Default library"), "path"),
        Setting("scan", "workers", _("Workers"), "int"),
        Setting("scan", "chunk_size", _("Chunk size"), "int"),
        Setting("scan", "skip_existing", _("Skip already indexed files"), "bool"),
        Setting("gui", "theme", _("Theme"), "choice", ("system", "light", "dark")),
        Setting("gui", "thumbnail_size", _("Thumbnail size"), "int"),
        Setting("gui", "window_width", _("Window width"), "int"),
        Setting("gui", "window_height", _("Window height"), "int"),
        Setting("locale", "language", _("Language"), "choice", ("auto", "en", "ru")),
        Setting("locale", "date_format", _("Date format"), "text"),
    ]


def bounds(setting: Setting) -> Tuple[int, int]:
    """The model's own ge/le for a number, so the two cannot drift apart.

    Capped: a spin box does not need a billion, and a smaller range keeps its width
    hint steady while a page is being shown.
    """
    field = MeretConfig.model_fields[setting.section].annotation.model_fields[setting.name]
    low, high = 0, 100_000
    for rule in field.metadata:
        if (ge := getattr(rule, "ge", None)) is not None:
            low = max(low, int(ge))
        if (le := getattr(rule, "le", None)) is not None:
            high = min(high, int(le))
    return low, high


def apply_settings(values: Dict[str, Any]) -> None:
    """Write the edited values and save once.

    Not `manager.set()` per key: that guesses a type from the text, so a path like
    "8" would be turned into an int and rejected by an Optional[str] field - and it
    rewrites the file for every single field.
    """
    manager = get_config()
    data = manager.config.model_dump()
    for key, value in values.items():
        section, name = key.split(".", 1)
        data.setdefault(section, {})[name] = value
    manager._config = MeretConfig.model_validate(data)
    manager.save()


class SettingsDialog(QDialog):
    """Every setting as a widget, one tab per section."""

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.setWindowTitle(_("Settings"))
        # A fixed size, so switching pages cannot resize the window: the pages differ
        # in height, and a dialog that grows on every tab click is a layout that never
        # settles.
        self.resize(640, 540)

        manager = get_config()
        self._original: Dict[str, Any] = {
            setting.key: manager.get(setting.key) for setting in settings_table()
        }
        self._editors: Dict[str, QWidget] = {}

        tabs = QTabWidget(self)
        for section, title in sections():
            tabs.addTab(self._page(section), title)

        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel,
            parent=self,
        )
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        open_file = buttons.addButton(
            _("Open the file"), QDialogButtonBox.ButtonRole.ActionRole
        )
        open_file.clicked.connect(lambda _checked=False: manager.edit())

        layout = QVBoxLayout(self)
        layout.addWidget(tabs)
        layout.addWidget(QLabel(str(manager.config_path)))
        layout.addWidget(buttons)

    def _page(self, section: str) -> QWidget:
        """One tab: a form with a row per setting of that section.

        Parentless on purpose: addTab() adopts the page, and a page that is also a
        child of the dialog would be laid out twice.

        DontWrapRows is the important line. With wrapping, the form reports
        heightForWidth; a stacked widget is poor at that, the dialog re-lays itself
        out on every page change, and after a few switches it spins instead of
        settling - which is exactly the freeze in the app.
        """
        page = QWidget()
        form = QFormLayout(page)
        form.setRowWrapPolicy(QFormLayout.RowWrapPolicy.DontWrapRows)
        for setting in settings_table():
            if setting.section == section:
                form.addRow(setting.label, self._editor(setting))
        return page

    def _editor(self, setting: Setting) -> QWidget:
        """The widget this field's kind asks for, filled with its current value.

        Every branch registers its value holder in self._editors, so values() reads
        from one place no matter how the row is laid out.
        """
        value = self._original[setting.key]

        if setting.kind == "bool":
            box = QCheckBox(self)
            box.setChecked(bool(value))
            self._editors[setting.key] = box
            return box

        if setting.kind == "int":
            spin = QSpinBox(self)
            low, high = bounds(setting)
            spin.setRange(low, high)
            spin.setValue(int(value) if value is not None else low)
            self._editors[setting.key] = spin
            return spin

        if setting.kind == "choice":
            combo = QComboBox(self)
            # AdjustToContentsOnFirstShow measures the popup while the page is being
            # shown, which re-runs the layout it is measuring
            combo.setSizeAdjustPolicy(QComboBox.SizeAdjustPolicy.AdjustToContents)
            combo.addItems(setting.choices)
            if value in setting.choices:
                combo.setCurrentText(str(value))
            self._editors[setting.key] = combo
            return combo

        line = QLineEdit(self)
        line.setText("" if value is None else str(value))
        self._editors[setting.key] = line

        if setting.kind != "path":
            return line

        line.setPlaceholderText(_("auto"))
        row = QWidget()
        browse = QPushButton("…", row)
        browse.setFixedWidth(32)
        browse.clicked.connect(
            lambda _checked=False, item=setting, target=line: self._browse(item, target)
        )
        layout = QHBoxLayout(row)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(line)
        layout.addWidget(browse)
        return row

    def _browse(self, setting: Setting, line: QLineEdit) -> None:
        """Pick a folder or a file, depending on what the setting points at."""
        start = line.text() or str(Path.home())
        if setting.name == "default_library":
            chosen = QFileDialog.getExistingDirectory(self, setting.label, start)
        else:
            chosen, _filters = QFileDialog.getSaveFileName(self, setting.label, start)
        if chosen:
            line.setText(chosen)

    def values(self) -> Dict[str, Any]:
        """The edited values, by dot-key. Empty path fields become None."""
        result: Dict[str, Any] = {}
        for setting in settings_table():
            editor = self._editors[setting.key]
            if isinstance(editor, QCheckBox):
                result[setting.key] = editor.isChecked()
            elif isinstance(editor, QSpinBox):
                result[setting.key] = editor.value()
            elif isinstance(editor, QComboBox):
                result[setting.key] = editor.currentText()
            else:
                text = editor.text().strip()
                result[setting.key] = (text or None) if setting.kind == "path" else text
        return result