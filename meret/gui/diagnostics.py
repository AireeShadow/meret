"""Diagnostics: the traced application and the media-log switch.

Nothing here touches the database or the panels. These are the parts of the GUI that
exist to explain the GUI, kept out of the window so that reading the window does not
mean reading the watchdog.
"""

import os
import sys
import threading
import time

from PySide6.QtWidgets import QApplication
from PySide6.QtCore import QTimer

from meret.core.debug import enabled

#: Qt logging rules that keep the FFmpeg backend quiet. It announces its version, its
#: codecs and every misdetection on stderr, which for a window with no console is noise
#: in whatever terminal started it. Warnings and errors stay: a file that will not open
#: is still worth reading. --debug media_log turns the whole category back on.
QUIET_MEDIA_RULES = "qt.multimedia.ffmpeg*.debug=false;qt.multimedia.ffmpeg*.info=false"


def quiet_media_logs() -> None:
    """Silence the media backend before Qt loads it.

    QT_LOGGING_RULES is read when the application is created, so this has to run
    before QApplication - a rule set afterwards misses the plugin's own start-up
    lines. A value already in the environment is kept and ours is appended to it.
    """
    rule = "qt.multimedia.*=true" if enabled("media_log") else QUIET_MEDIA_RULES
    existing = os.environ.get("QT_LOGGING_RULES", "")
    os.environ["QT_LOGGING_RULES"] = f"{existing};{rule}" if existing else rule


class TracedApplication(QApplication):
    """An application that counts events and names whatever floods them.

    A freeze the suite cannot reproduce has two shapes, and they need opposite
    fixes: the loop stops turning - one event never returns - or it turns far too
    fast, a layout or paint storm. The rate tells them apart, and the per-receiver
    table names the widget that has to be fixed.

    The report goes to stderr through print(), never through logging: the CLI may
    have pointed logging at a file, and a diagnostic that can be swallowed is worse
    than none. It is printed by a daemon thread, so the output keeps coming even
    when the GUI thread is the thing that is stuck - a frozen counter is the answer.
    """

    SLOW_MS = 150.0
    REPORT_MS = 1000
    TOP = 6

    def __init__(self, argv: list[str]) -> None:
        super().__init__(argv)
        self._events = 0
        self._worst = 0.0
        self._worst_where = "-"
        self._counts: dict[tuple, int] = {}
        self._chains: dict[tuple, str] = {}
        self._last_report = time.perf_counter()
        self._text = ""
        # The event the GUI thread is inside right now, and when it last finished
        # one. A handler that never returns never reaches notify()'s finally, so
        # these two are the only witnesses a permanent freeze can leave behind.
        self._current = "-"
        self._last_event_done = time.perf_counter()

        self._beat = QTimer(self)
        self._beat.setInterval(self.REPORT_MS)
        self._beat.timeout.connect(self._report)
        self._beat.start()

        threading.Thread(target=self._watch, daemon=True).start()

    def notify(self, receiver, event) -> bool:
        """Time every event and count it per receiver, so a storm names itself."""
        started = time.perf_counter()
        # Read by the watchdog thread: the only witness when an event never returns
        self._current = f"{type(receiver).__name__}/{type(event).__name__}"
        try:
            return super().notify(receiver, event)
        finally:
            self._current = "-"
            self._last_event_done = time.perf_counter()
            spent = (time.perf_counter() - started) * 1000
            self._events += 1
            pair = (type(receiver).__name__, type(event).__name__)
            self._counts[pair] = self._counts.get(pair, 0) + 1
            if pair not in self._chains:
                self._chains[pair] = self._chain(receiver)
            if spent > self._worst:
                self._worst = spent
                self._worst_where = f"{pair[0]}/{pair[1]}"
            if spent >= self.SLOW_MS:
                print(
                    f"[watchdog] slow: {pair[1]} took {spent:.0f} ms in {self._chains[pair]}",
                    file=sys.stderr,
                    flush=True,
                )

    @staticmethod
    def _chain(receiver) -> str:
        """The receiver and its ancestors: a busy widget has to name its window."""
        names: list[str] = []
        node = receiver
        while node is not None and len(names) < 6:
            names.append(type(node).__name__)
            try:
                node = node.parent() if hasattr(node, "parent") else None
            except RuntimeError:
                break  # the C++ object is already gone, e.g. a finished animation
        return " < ".join(names)

    def _report(self) -> None:
        """Once a second, on the GUI thread: compose the line the thread prints."""
        now = time.perf_counter()
        gap = (now - self._last_report) * 1000
        busiest = sorted(self._counts.items(), key=lambda item: item[1], reverse=True)
        lines = [
            (
                f"{self._events} events/s | worst {self._worst:.0f} ms"
                f" ({self._worst_where}) | tick gap {gap:.0f} ms"
                f" | active {self._name_of(self.activeWindow())}"
                f" | modal {self._name_of(self.activeModalWidget())}"
                f" | shown {[type(w).__name__ for w in self.topLevelWidgets() if w.isVisible()]}"
            )
        ]
        for pair, count in busiest[: self.TOP]:
            lines.append(f"    {count:>8}  {pair[1]} in {self._chains[pair]}")
        self._text = "\n".join(lines)

        self._events = 0
        self._worst = 0.0
        self._worst_where = "-"
        self._counts = {}
        self._chains = {}
        self._last_report = now

    def _watch(self) -> None:
        """A daemon thread: keep printing while the GUI thread cannot."""
        while True:
            time.sleep(self.REPORT_MS / 1000 * 2)
            if self._text:
                print(f"[watchdog]\n{self._text}", file=sys.stderr, flush=True)
            # A handler that never returns never reaches notify()'s finally, so the
            # event counter freezes and no "slow" line is ever printed - the freeze
            # is silent exactly when it is worst. This line names it instead.
            stuck_for = time.perf_counter() - self._last_event_done
            if self._current != "-" and stuck_for > 2.0:
                print(
                    f"[watchdog] stuck for {stuck_for:.1f} s in: {self._current}",
                    file=sys.stderr,
                    flush=True,
                )

    @staticmethod
    def _name_of(widget) -> str:
        """A widget's class, or a dash when there is none."""
        return "-" if widget is None else type(widget).__name__
