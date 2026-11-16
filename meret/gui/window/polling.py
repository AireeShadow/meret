"""Watching the database for changes made by other processes."""

import logging

from typing import TYPE_CHECKING

from PySide6.QtCore import QTimer

logger = logging.getLogger(__name__)

#: How often the database's data_version is checked for outside changes.
POLL_MS = 1500

if TYPE_CHECKING:
    from meret.gui.window.base import WindowBase
else:
    WindowBase = object


class PollMixin(WindowBase):
    """The poll that picks up tagging done by the CLI, a mount or a scan."""

    def _start_poll(self) -> None:
        """Create and start the timer. Called once, from WindowBase.__init__."""
        self._poll = QTimer(self)
        self._poll.setInterval(POLL_MS)
        self._poll.timeout.connect(self._check_version)
        self._poll.start()

    def _check_version(self) -> None:
        """Pick up tagging done by another process (the CLI, a mount, a scan).

        Sticky: every folder change bumps the generation, so a probe that got
        dropped each time would never report anything while the user browses.
        """
        self._tasks.run(self._repo.data_version, self._on_version, sticky=True)

    def _on_version(self, version: int | None) -> None:
        if version is None or version == self._version:
            return
        known = self._version is not None
        self._version = version
        if not known:
            return  # the first reading is just a baseline
        logger.debug("data_version moved to %s, reloading", version)
        self._refresh()
