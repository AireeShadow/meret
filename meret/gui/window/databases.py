"""Opening, wiping and switching the database.

Every method here either runs on the task pool (opening, wiping) or touches Qt only
after the pool has answered (swapping the repository in). That split is the whole
point: a lock left by a task in flight used to freeze the window for half a second
while it retried, and a wipe used to leave a brand-new database in `delete` journal
mode, where switching to WAL wants an exclusive lock PRAGMA does not wait for.
"""

import gc
import logging
import sqlite3
import time
from pathlib import Path
from typing import TYPE_CHECKING

from PySide6.QtWidgets import QFileDialog, QMessageBox

from meret.core.locale import _
from meret.core.sqlite_repo import SQLiteRepository
from meret.gui.thumbnails import ThumbnailCache
from meret.vfs.tree import VirtualTree

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from meret.gui.window.base import WindowBase
else:
    WindowBase = object


class DatabaseMixin(WindowBase):
    """`init_db`, the switch to another database, and the wipe that precedes it."""

    def _init_db(self) -> None:
        """Create the database - or ask what to do when one is already here.

        Doing nothing was the bug: the user clicks, the file exists, and no window,
        no message, nothing at all.
        """
        if not self._db_path.exists():
            self._use_db(self._db_path)
            self._say(_("Database initialized!"))
            return

        box = QMessageBox(self)
        box.setWindowTitle(_("Initialize new database"))
        box.setIcon(QMessageBox.Icon.Question)
        box.setText(_("A database already exists here:"))
        box.setInformativeText(str(self._db_path))
        here = box.addButton(_("Recreate here"), QMessageBox.ButtonRole.AcceptRole)
        elsewhere = box.addButton(_("Choose a folder..."), QMessageBox.ButtonRole.ActionRole)
        cancel = box.addButton(_("Cancel"), QMessageBox.ButtonRole.RejectRole)
        box.setDefaultButton(here)
        box.setEscapeButton(cancel)
        box.exec()

        clicked = box.clickedButton()
        if clicked is here:
            self._use_db(self._db_path, fresh=True)
            self._say(_("Database recreated!"))
        elif clicked is elsewhere:
            self._create_elsewhere()

    def _use_db(self, path: Path, fresh: bool = False) -> None:
        """Point the whole window at another database, off the GUI thread.

        Opening and wiping both retry with a sleep while a task in flight still holds
        the file - up to half a second of a frozen window. So the work goes to the
        pool and the swap happens in the callback: it touches Qt, so it has to stay
        here. The task is sticky, because the switch must land even if the user
        browses while it runs.
        """
        path = Path(path)
        old = self._repo
        self._tasks.run(
            self._prepare_db,
            on_done=lambda repo: self._switch_to(repo, path),
            on_failed=self._on_db_failed,
            args=(old, path, fresh),
            sticky=True,
        )

    def _prepare_db(self, old, path: Path, fresh: bool):
        """Pool side: wipe when asked, then open. Neither touches Qt."""
        if fresh:
            self._wipe(old, path)
        return self._open_db(path)

    def _switch_to(self, repo, path: Path) -> None:
        """The new database is open: swap it in and re-read everything.

        Nothing is set to None on the way: a task in flight can call back at any
        moment, and it reads self._repo and self._tree.
        """
        self._repo = repo
        self._db_path = path
        self._tree = VirtualTree(repo)
        self._thumbs = ThumbnailCache(size=self._thumbs.size, scope=str(self._db_path))

        self.tags.set_tree(self._tree)
        self._version = None
        self.open_folder("/")
        self._show_preview()

    def _on_db_failed(self, error: Exception) -> None:
        """A database that would not open is a status line, not a crash.

        The old database stays in place: nothing was swapped yet.
        """
        logger.warning("cannot switch database: %s", error)
        self._say(_("Error: {error}").format(error=error))

    def _open_db(self, path: Path):
        """Open a repository, waiting out a lock left by a task in flight.

        Runs on the pool, so the sleep below is a wait, never a freeze. A scan writes
        while it runs, so its connection can hold the file for a moment; connections
        are opened per operation, so the wait is short and the retry is a safety net.
        """
        for _attempt in range(10):
            try:
                return SQLiteRepository(str(path))
            except sqlite3.OperationalError as error:
                if "locked" not in str(error) and "busy" not in str(error):
                    raise
                gc.collect()
                time.sleep(0.05)
        return SQLiteRepository(str(path))

    def _wipe(self, old, path: Path) -> None:
        """Empty the database through SQL, leaving its file alone.

        Runs on the pool, like _open_db. Deleting the file would leave a brand-new
        database in `delete` journal mode, and switching that to WAL wants an
        exclusive lock that PRAGMA journal_mode does not wait for - that was the
        `database is locked`. Dropping the objects keeps the file, and its WAL mode,
        exactly where they are, and the schema is rebuilt by the repository itself.
        """
        closer = getattr(old, "close", None)
        if closer is not None:
            closer()

        for _attempt in range(10):
            try:
                self._drop_objects(path)
                return
            except sqlite3.OperationalError as error:
                if "locked" not in str(error) and "busy" not in str(error):
                    raise
                gc.collect()
                time.sleep(0.05)
        self._drop_objects(path)

    @staticmethod
    def _drop_objects(path: Path) -> None:
        """Drop every table and view, creating the file when it is not there yet.

        One connection of our own, closed on the way out: the repository opens its
        own per operation, and a handle left behind is one more lock to wait for.
        """
        conn = sqlite3.connect(str(path))
        try:
            # Off first: with it on, the drops would depend on the reference order
            conn.execute("PRAGMA foreign_keys = OFF")
            rows = list(
                conn.execute(
                    "SELECT type, name FROM sqlite_master"
                    " WHERE type IN ('view', 'table') AND name NOT LIKE 'sqlite_%'"
                    " ORDER BY type = 'table'"  # views first, their tables after
                )
            )
            for kind, name in rows:
                conn.execute(f'DROP {kind.upper()} IF EXISTS "{name}"')
            conn.commit()
        finally:
            conn.close()

    def _create_elsewhere(self) -> None:
        """Pick a folder, put a fresh database in it, and work on that one."""
        folder = QFileDialog.getExistingDirectory(
            self, _("Choose a folder for the database"), str(self._db_path.parent)
        )
        if not folder:
            return
        target = Path(folder) / self._db_path.name
        if target.exists() and not self._confirm_overwrite(target):
            return
        self._use_db(target, fresh=True)
        self._say(_("Database: {path}").format(path=target))

    def _confirm_overwrite(self, target: Path) -> bool:
        """Yes/no for a database that already sits in the chosen folder."""
        answer = QMessageBox.question(
            self,
            _("Initialize new database"),
            _("A database already exists there:\n{path}\n\nRecreate it?").format(path=target),
        )
        return answer == QMessageBox.StandardButton.Yes
