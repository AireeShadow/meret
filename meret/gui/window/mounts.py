"""Mounting and unmounting: shelling out to the CLI, never importing a backend.

Keeping `meret.vfs.backends` out of `meret.gui` means opening a window never drags
FUSE or WinFsp in. Everything that waits for a process runs on the task pool: a
blocking subprocess.run() on the GUI thread is a window that stops answering.
"""

import logging
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from PySide6.QtWidgets import QInputDialog

from meret.core.locale import _

logger = logging.getLogger(__name__)

#: How long a CLI call may take before the pool gives up on it. `meret umount` finds
#: the mount process with PowerShell on Windows, and a stuck WMI call has no natural
#: end - without this the task would hold its pool thread forever.
CLI_TIMEOUT = 30.0

if TYPE_CHECKING:
    from meret.gui.window.base import WindowBase
else:
    WindowBase = object


class MountMixin(WindowBase):
    """`mount`, `umount`, and the CLI plumbing they share."""

    def _mount(self) -> None:
        """Start `meret mount` in its own process, so `meret umount` can stop it.

        The backend is deliberately not imported: keeping it out of meret.gui means
        no FUSE or WinFsp import just to open a window.
        """
        default = "M:" if sys.platform == "win32" else str(Path.home() / "meret-tags")
        point, accepted = QInputDialog.getText(
            self, _("Mount virtual filesystem"), _("Where to mount, e.g. M:"), text=default
        )
        if not accepted or not point.strip():
            return
        self._say(_("Mounting at {path}...").format(path=point.strip()))
        self._spawn(self._cli("mount", point.strip()))

    def _umount(self) -> None:
        """Stop the mount process. It takes no --db, only the mount point.

        The CLI runs on the pool, never here. `meret umount` finds its target with
        PowerShell on Windows - seconds of WMI enumeration, unbounded when the WMI
        service is unhappy - and subprocess.run() on the GUI thread is a window that
        cannot repaint for exactly that long.
        """
        point, accepted = QInputDialog.getText(
            self, _("Unmount"), _("Mount point, e.g. M: or K:\\tags")
        )
        if not accepted or not point.strip():
            return

        point = point.strip()
        self._say(_("Unmounting {path}...").format(path=point))

        def done(result: tuple[int, str]) -> None:
            self._on_umount_done(point, result)

        self._run_async(self._cli("umount", point, with_db=False), done)

    def _on_umount_done(self, point: str, result: tuple[int, str]) -> None:
        """Say what actually happened.

        The old code announced "Unmounted" before the command returned, so an unmount
        that found nothing looked like a success. The CLI exits 1 with "Nothing is
        mounted at ..." and that sentence is the honest answer.
        """
        code, message = result
        if code == 0:
            self._say(_("Unmounted {path}").format(path=point))
            return
        logger.info("umount %s: exit %s: %s", point, code, message)
        self._say(message or _("Nothing is mounted at {path}").format(path=point))

    def _cli(self, *args: str, with_db: bool = True) -> list[str]:
        """A `meret` command line.

        `with_db=False` for `umount`: it finds the mount process by the mount point
        alone, so `umount_cmd` declares no --db and typer rejects an unknown option.
        """
        command = ["meret", *args]
        if with_db:
            command += ["--db", str(self._db_path)]
        return command

    @staticmethod
    def _cli_call(command: list[str]) -> tuple[int, str]:
        """Run a `meret` command to completion. Pool side only.

        Never call this on the GUI thread: subprocess.run() blocks, and a blocked GUI
        thread is a window that stops answering. `stdin=DEVNULL` because a child that
        decides to prompt would otherwise wait on a pipe nobody closes, and `timeout`
        because a child stuck in the WMI service would wait forever.
        """
        logger.info("run: %s", command)
        try:
            done = subprocess.run(
                command,
                capture_output=True,
                text=True,
                stdin=subprocess.DEVNULL,
                timeout=CLI_TIMEOUT,
            )
        except subprocess.TimeoutExpired:
            logger.warning("timeout after %.0f s: %s", CLI_TIMEOUT, command)
            return -1, _("Timed out after {seconds:.0f} s").format(seconds=CLI_TIMEOUT)
        except OSError as e:
            logger.warning("cannot run %s: %s", command, e)
            return -1, _("Error: {error}").format(error=e)
        if done.returncode != 0:
            logger.warning("exit %s: %s", done.returncode, (done.stderr or "").strip())
        message = (done.stdout or "").strip() or (done.stderr or "").strip()
        return done.returncode, message

    def _run_async(self, command: list[str], on_done) -> None:
        """Run a CLI command on the pool and hand the result back on the GUI thread.

        Sticky: an unmount must land even if the user browses while it runs, and a
        folder change bumps the generation of everything else.
        """
        self._tasks.run(
            self._cli_call,
            on_done=on_done,
            on_failed=self._on_cli_failed,
            args=(command,),
            sticky=True,
        )

    def _on_cli_failed(self, error: Exception) -> None:
        """A CLI call that raised is a status line, not a crash."""
        logger.warning("CLI call failed: %s", error)
        self._say(_("Error: {error}").format(error=error))

    def _spawn(self, command: list[str]) -> None:
        """Run without waiting - a mount lives until it is unmounted.

        `stdin=DEVNULL`: the mount process waits for Enter on Windows, and a window
        has no console to inherit one from. Leaving it attached is how a mount ends
        the moment it starts.
        """
        logger.info("run: %s", command)
        try:
            subprocess.Popen(command, stdin=subprocess.DEVNULL)
        except OSError as e:
            logger.warning("cannot run %s: %s", command, e)
            self._say(_("Error: {error}").format(error=e))
