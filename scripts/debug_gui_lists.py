"""Scratch check: does the GUI grid fill from a virtual folder?"""
from __future__ import annotations

import os
import sys
import tempfile
import time
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PIL import Image
from PySide6.QtWidgets import QApplication

from meret.core.models import FileEntry, FileType
from meret.core.sqlite_repo import SQLiteRepository
from meret.gui.app import MainWindow


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

    print("listdir /travel ->", window._tree.listdir("/travel"))
    window.open_folder("/travel")
    for _ in range(200):
        app.processEvents()
        time.sleep(0.01)
    print("grid count ->", window.grid.count())
    print("status ->", window._status_label.text())
    return 0


if __name__ == "__main__":
    sys.exit(main())