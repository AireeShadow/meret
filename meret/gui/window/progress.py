"""A modal progress window for work that runs on the task pool.

The work calls its progress callback on a pool thread, so the callback cannot touch a
widget. It emits `advanced` instead: Qt queues a signal from a worker thread to the
receiver's thread, which is exactly the hop that is needed - and the reason the callback
is the signal's `emit` and not a method that updates the bar.

The window is opened, never exec()'d. exec() runs a nested event loop, and this window
has already paid for that once: see _edit_config, where a dialog under modality left
the app looking dead because Qt dropped every click on the window behind it.
"""

import logging

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import QDialog, QLabel, QProgressBar, QVBoxLayout, QWidget

from meret.core.indexer import PHASE_COLLECT, PHASE_EXTRACT, PHASE_INDEX
from meret.core.locale import _

logger = logging.getLogger(__name__)


def _phase_label(phase: str) -> str:
    """Translated name of a scan phase. Resolved at call time, not at import."""
    table = {
        PHASE_COLLECT: _("Finding files"),
        PHASE_EXTRACT: _("Reading metadata"),
        PHASE_INDEX: _("Writing to database"),
    }
    return table.get(phase, "")


class ProgressDialog(QDialog):
    """A label and a bar, fed by a pool thread through a queued signal.

    `total == 0` keeps the bar in its busy state: a run that has not counted its work
    yet must not claim to be at 0% of something.
    """

    advanced = Signal(str, int, int)

    def __init__(self, title: str, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setWindowTitle(title)
        self.setWindowModality(Qt.WindowModality.ApplicationModal)
        self.setMinimumWidth(420)
        # The "?" in the title bar opens a help window that does not exist
        self.setWindowFlag(Qt.WindowType.WindowContextHelpButtonHint, False)

        self._label = QLabel("", self)
        self._bar = QProgressBar(self)
        self._bar.setRange(0, 0)  # busy until the first real total arrives

        layout = QVBoxLayout(self)
        layout.addWidget(self._label)
        layout.addWidget(self._bar)

        self.advanced.connect(self._on_advanced)

    def report(self, phase: str, done: int, total: int) -> None:
        """The callback to hand to the pool. Safe from any thread."""
        self.advanced.emit(phase, done, total)

    def _on_advanced(self, phase: str, done: int, total: int) -> None:
        """GUI thread: one step arrived."""
        if phase:
            self._label.setText(_phase_label(phase))
        if total > 0:
            self._bar.setRange(0, total)
            self._bar.setValue(done)
