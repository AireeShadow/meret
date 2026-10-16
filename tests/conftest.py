"""
Pytest configuration and shared fixtures.
"""
import logging
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from PIL import Image

from meret.core.config import ConfigManager
from meret.core.models import FileEntry, FileType
from meret.core.sqlite_repo import SQLiteRepository
from tests.mocks import MockRepository


@pytest.fixture(autouse=True)
def clean_logger():
    """The logger is global — clear its handlers between tests."""
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
    """Both implementations run through the same tests."""
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


@pytest.fixture
def config_manager(tmp_path: Path) -> ConfigManager:
    """Fresh config manager in temp directory."""
    return ConfigManager(tmp_path / "test_config.toml")


@pytest.fixture
def isolated_config(monkeypatch, tmp_path: Path):
    """
    Isolate config from user environment.
    Use this when testing config modifications.
    """
    import meret.core.config as config_module
    
    config_dir = tmp_path / "meret_config"
    config_dir.mkdir()
    
    monkeypatch.setattr(
        config_module,
        "platformdirs",
        MagicMock(user_config_dir=lambda x: str(config_dir))
    )
    
    # Reset singleton
    config_module._config_instance = None
    
    yield config_dir
    
    # Cleanup
    config_module._config_instance = None