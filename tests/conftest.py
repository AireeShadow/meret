import logging
from pathlib import Path

import pytest
from PIL import Image

from meret.core.models import FileEntry, FileType
from meret.core.sqlite_repo import SQLiteRepository
from tests.mocks import MockRepository


@pytest.fixture(autouse=True)
def clean_logger():
    """Логгер глобальный — чистим хендлеры между тестами."""
    logger = logging.getLogger("meret")
    logger.handlers.clear()
    yield logger
    logger.handlers.clear()


@pytest.fixture
def sqlite_repo(tmp_path: Path) -> SQLiteRepository:
    return SQLiteRepository(str(tmp_path / "test.db"))


@pytest.fixture
def mock_repo() -> MockRepository:
    return MockRepository()


@pytest.fixture(params=["sqlite", "mock"])
def repo(request, sqlite_repo, mock_repo):
    """Обе реализации гоняются через одни и те же тесты."""
    return sqlite_repo if request.param == "sqlite" else mock_repo


@pytest.fixture
def make_png(tmp_path: Path):
    counter = {"n": 0}

    def _make(name: str = "photo", size=(16, 16)) -> Path:
        counter["n"] += 1
        path = tmp_path / f"{name}_{counter['n']}.png"
        Image.new("RGB", size, "red").save(path)
        return path

    return _make


@pytest.fixture
def sample_file() -> FileEntry:
    return FileEntry(filename="photo.png", filepath="/media/photo.png", type=FileType.IMAGE)