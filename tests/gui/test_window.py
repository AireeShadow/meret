"""Smoke test: the window builds, lists a tagged file and closes cleanly."""
import pytest

from tests.gui.conftest import pump

pytest.importorskip("PySide6")


def test_window_lists_tagged_files(tmp_path, qapp, qtbot):
    from PIL import Image

    from meret.core.models import FileEntry, FileType
    from meret.core.sqlite_repo import SQLiteRepository
    from meret.gui.app import MainWindow

    photo = tmp_path / "photo.png"
    Image.new("RGB", (32, 32), "red").save(photo)

    repo = SQLiteRepository(str(tmp_path / "meret.db"))
    file_id = repo.save_file(
        FileEntry(filename=photo.name, filepath=str(photo), type=FileType.IMAGE)
    )
    repo.add_tag(file_id, "travel")

    window = MainWindow(repo, tmp_path / "meret.db")
    window.open_folder("/travel")

    qtbot.waitUntil(lambda: window.grid.count() == 1, timeout=2000)

    assert window.grid.item(0).text() == "photo.png"

def test_pump_accepts_app_positionally(qapp):
    from tests.gui.conftest import pump

    pump(qapp)
    pump(10)
    pump(ms=5)

def test_window_constructs_without_missing_state(tmp_path, qapp):
    from meret.core.sqlite_repo import SQLiteRepository
    from meret.gui.app import MainWindow

    repo = SQLiteRepository(str(tmp_path / "meret.db"))
    window = MainWindow(repo, tmp_path / "meret.db")

    assert window.grid.count() == 0  # an empty repo has nothing to show