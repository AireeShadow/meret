"""What QFileSystemModel actually reports at its own root.

    python scripts/debug_fs_model.py

The panel showed a single row instead of the drive list, and the log says rootPath is
"" while rowCount() is 1. This prints what the model returns at the top, what
QDir.drives() says the machine has, and whether a re-read changes either - so the fix
stops being a guess.
"""

import sys

from PySide6.QtCore import QDir, QModelIndex
from PySide6.QtWidgets import QApplication, QFileSystemModel


def dump(model: QFileSystemModel, label: str) -> None:
    """The root of the model: path, row count and the first rows it offers."""
    root = QModelIndex()
    rows = model.rowCount(root)
    print(f"--- {label}")
    print(f"    rootPath={model.rootPath()!r} rowCount={rows} canFetchMore={model.canFetchMore(root)}")
    for row in range(min(rows, 12)):
        index = model.index(row, 0, root)
        print(f"    [{row}] filePath={model.filePath(index)!r} name={model.fileInfo(index).fileName()!r}")


def main() -> int:
    app = QApplication(sys.argv)
    print("drives:", [drive.absolutePath() for drive in QDir.drives()])
    print("home:", QDir.homePath())
    print("root:", QDir.rootPath())

    model = QFileSystemModel()
    model.setOption(QFileSystemModel.Option.DontWatchForChanges, True)
    model.setOption(QFileSystemModel.Option.DontUseCustomDirectoryIcons, True)

    dump(model, "fresh model, rootPath untouched")
    model.setRootPath("")
    dump(model, "after setRootPath('')")
    model.setRootPath(QDir.homePath())
    model.setRootPath("")
    dump(model, "after setRootPath(home) -> setRootPath('')")

    for _ in range(100):
        app.processEvents()

    dump(model, "after pumping events")
    model.setRootPath(QDir.homePath())
    dump(model, "after setRootPath(home)")
    model.setRootPath("")
    dump(model, "after setRootPath(home) -> setRootPath('') again")

    print("top-level index valid:", model.index(0, 0, QModelIndex()).isValid())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())