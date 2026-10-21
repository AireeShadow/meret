"""Scratch check: does the runner deliver, and does the grid fill?"""
from __future__ import annotations

import logging
import os
import sys
import tempfile
import time
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logging.getLogger("meret.gui").setLevel(logging.DEBUG)

from PIL import Image
from PySide6.QtWidgets import QApplication

from meret.core.models import FileEntry, FileType
from meret.core.sqlite_repo import SQLiteRepository
from meret.gui.app import MainWindow


def spin(app, predicate, timeout_s: float = 3.0) -> bool:
    """Pump the event loop until predicate is true or the timeout expires."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        app.processEvents()
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()


def main() -> int:
    tmp = Path(tempfile.mkdtemp())
    photo = tmp / "photo.png"
    Image.new("RGB", (32, 32), "red").save(photo)

    db = tmp / "meret.db"
    repo = SQLiteRepository(str(db))
    file_id = repo.save_file(
        FileEntry(filename=photo.name, filepath=str(photo), type=FileType.IMAGE)
    )
    repo.add_tag(file_id, "travel")

    app = QApplication.instance() or QApplication([])
    window = MainWindow(repo, db)

    # 1) does the runner deliver anything at all?
    seen: list = []
    window._tasks.run(lambda: 42, on_done=seen.append)
    print("probe delivered ->", spin(app, lambda: bool(seen)), seen)

    # 2) does the grid fill?
    print("listdir /travel ->", len(window._tree.listdir("/travel")))
    window.open_folder("/travel")
    filled = spin(app, lambda: window.grid.count() > 0)
    print("grid filled ->", filled, "count ->", window.grid.count())
    print("queue left ->", len(window._queue))
    print("status bar ->", window.statusBar().currentMessage())
    return 0


if __name__ == "__main__":
    sys.exit(main())