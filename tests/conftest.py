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

pytest_plugins = ("pytest_asyncio",)


@pytest.fixture(autouse=True)
def english_and_isolated_config(tmp_path_factory, monkeypatch):
    """Whatever this machine's language and config are, tests see English and a temp config."""
    import meret.core.config as config_module
    import meret.core.locale as locale_module

    path = tmp_path_factory.mktemp("config") / "config.toml"
    monkeypatch.setattr(config_module, "_config_instance", config_module.ConfigManager(path))
    monkeypatch.setenv("LANGUAGE", "en")  # first thing system_language() looks at
    monkeypatch.delenv("MERET_DB", raising=False)  # a developer's own default db
    monkeypatch.setattr(locale_module, "_locale_manager", None)


@pytest.fixture(autouse=True)
def clean_logger(tmp_path_factory, monkeypatch):
    """Logging is global: keep CLI logs out of the user's log dir, undo setup_logging()."""
    from meret.core.logger import _MARK, ENV_FILE, ENV_LEVEL

    log_file = tmp_path_factory.mktemp("logs") / "meret.log"
    monkeypatch.setenv(ENV_FILE, str(log_file))
    monkeypatch.delenv(ENV_LEVEL, raising=False)

    root, logger = logging.getLogger(), logging.getLogger("meret")
    root_level = root.level
    yield logger

    for handler in [h for h in root.handlers if getattr(h, _MARK, False)]:
        root.removeHandler(handler)
        handler.close()
    root.setLevel(root_level)
    logger.setLevel(logging.NOTSET)


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

@pytest.fixture(autouse=True)
def suppress_textual_warnings():
    """Suppress Textual internal warnings."""
    import warnings
    warnings.filterwarnings(
        "ignore",
        message="coroutine 'DirectoryTree.watch_path' was never awaited",
        category=RuntimeWarning
    )
    yield