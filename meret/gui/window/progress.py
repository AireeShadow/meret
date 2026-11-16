"""A modal progress window for work that runs on the task pool.

The work calls its progress callback on a pool thread, so the callback cannot touch a
widget: it emits `advanced`, and Qt queues the signal to the GUI thread.

The report is throttled. The indexer and the tagger report once per file, and ten
thousand files is ten thousand queued events and ten thousand repaints - the window
stops answering long before the run ends, which reads as "closing the dialog froze it".

The title bar has no X. Cancelling is a button, and the button asks the work to stop:
a window that closes and leaves the work behind it is a lie, because the poll stays
stopped and the application looks dead.
"""

import logging
import time

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QDialog,
    QHBoxLayout,
    QLabel,
    QProgressBar,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

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
    """A label, a bar and a Cancel button, fed by a pool thread through a signal."""

    advanced = Signal(str, int, int)
    cancelled = Signal()

    #: How often a step reaches the GUI thread: ten a second is smooth to the eye.
    THROTTLE_S = 0.1

    def __init__(self, title: str, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setWindowTitle(title)
        self.setWindowModality(Qt.WindowModality.ApplicationModal)
        self.setMinimumWidth(420)
        self.setWindowFlag(Qt.WindowType.WindowContextHelpButtonHint, False)
        self.setWindowFlag(Qt.WindowType.WindowCloseButtonHint, False)

        self._label = QLabel("", self)
        self._bar = QProgressBar(self)
        self._bar.setRange(0, 0)  # busy until the first real total arrives

        self._cancel = QPushButton(_("Cancel"), self)
        self._cancel.clicked.connect(self._on_cancel)

        buttons = QHBoxLayout()
        buttons.addStretch(1)
        buttons.addWidget(self._cancel)

        layout = QVBoxLayout(self)
        layout.addWidget(self._label)
        layout.addWidget(self._bar)
        layout.addLayout(buttons)

        # Written on the GUI thread, read from the pool: a bool, so no lock is needed
        self._finished = False
        self._last_emit = 0.0

        self.advanced.connect(self._on_advanced)

    # ---------- from the pool ----------

    def report(self, phase: str, done: int, total: int) -> None:
        """The callback to hand to the pool. Safe from any thread, and throttled.

        The last step is always sent, whatever the clock says: a bar that stops at 97%
        because the throttle ate the final event is worse than one that skips a tick.
        """
        if self._finished:
            return
        now = time.monotonic()
        if now - self._last_emit < self.THROTTLE_S and done != total:
            return
        self._last_emit = now
        self.advanced.emit(phase, done, total)

    # ---------- on the GUI thread ----------

    def _on_advanced(self, phase: str, done: int, total: int) -> None:
        """GUI thread: one step arrived."""
        if self._finished:
            return
        if phase:
            self._label.setText(_phase_label(phase))
        if total > 0:
            self._bar.setRange(0, total)
            self._bar.setValue(done)

    def _on_cancel(self) -> None:
        """Ask the work to stop, and say so: a second click does nothing."""
        self._cancel.setEnabled(False)
        self._label.setText(_("Cancelling..."))
        self.cancelled.emit()

    def finish(self) -> None:
        """Close for real. Called from the completion callback, never by the user."""
        self._finished = True
        self.close()

    # ---------- the window refuses to leave early ----------

    def reject(self) -> None:
        """Esc must not close a window whose work is still running."""
        if self._finished:
            super().reject()

    def closeEvent(self, event) -> None:
        """The title bar's X is gone; this is the belt for the same trousers."""
        if self._finished:
            super().closeEvent(event)
            return
        event.ignore()
