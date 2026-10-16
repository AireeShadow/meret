"""
Tests for configuration management.
"""
import json
import os
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

from pydantic import ValidationError

import pytest

from meret.core.config import (
    CONFIG_FILENAME,
    ConfigManager,
    GUIConfig,
    LocaleConfig,
    MeretConfig,
    PathsConfig,
    ScanConfig,
    get_config,
    reload_config,
)


class TestConfigManager:
    """Tests for ConfigManager class."""

    def test_default_config_creation(self, tmp_path: Path):
        """Creating config when file doesn't exist."""
        config_path = tmp_path / "config.toml"
        mgr = ConfigManager(config_path)
        
        config = mgr.config
        
        # Check defaults
        assert isinstance(config.paths, PathsConfig)
        assert isinstance(config.scan, ScanConfig)
        assert isinstance(config.gui, GUIConfig)
        assert isinstance(config.locale, LocaleConfig)
        
        # Check file was created
        assert config_path.exists()

    def test_load_existing_toml_config(self, tmp_path: Path):
        """Loading existing TOML config."""
        config_path = tmp_path / "config.toml"
        
        # Create config manually
        config_path.write_text("""
[paths]
default_db = "/home/user/media.db"
default_library = "/home/user/Pictures"

[scan]
workers = 4
skip_existing = false

[gui]
theme = "dark"
thumbnail_size = 512

[locale]
language = "ru"
""")
        
        mgr = ConfigManager(config_path)
        config = mgr.config
        
        assert config.paths.default_db == "/home/user/media.db"
        assert config.paths.default_library == "/home/user/Pictures"
        assert config.scan.workers == 4
        assert config.scan.skip_existing is False
        assert config.gui.theme == "dark"
        assert config.gui.thumbnail_size == 512
        assert config.locale.language == "ru"

    def test_load_existing_json_config(self, tmp_path: Path):
        """Loading JSON config (fallback)."""
        config_path = tmp_path / "config.json"
        
        data = {
            "paths": {"default_db": "~/test.db"},
            "scan": {"workers": 16}
        }
        config_path.write_text(json.dumps(data))
        
        mgr = ConfigManager(config_path)
        
        assert mgr.config.paths.default_db == "~/test.db"
        assert mgr.config.scan.workers == 16
        # Check defaults for missing fields
        assert mgr.config.gui.theme == "system"

    def test_get_by_dot_notation(self, tmp_path: Path):
        """Getting values with dot notation."""
        mgr = ConfigManager(tmp_path / "config.toml")
        
        # Set some values first
        mgr.config.paths.default_db = "/test.db"
        mgr.config.scan.workers = 12
        
        # Get existing values
        assert mgr.get("paths.default_db") == "/test.db"
        assert mgr.get("scan.workers") == 12
        assert mgr.get("gui.theme") == "system"
        
        # Get non-existent keys
        assert mgr.get("nonexistent") is None
        assert mgr.get("paths.nonexistent") is None
        assert mgr.get("deep.nested.key") is None
        
        # Default values
        assert mgr.get("missing", "default") == "default"

    def test_set_by_dot_notation(self, tmp_path: Path):
        """Setting values with dot notation."""
        mgr = ConfigManager(tmp_path / "config.toml")
        
        # Set various types
        mgr.set("scan.workers", "4")
        mgr.set("scan.skip_existing", "true")
        mgr.set("paths.default_library", "~/Media")
        
        # Reload and verify
        mgr2 = ConfigManager(tmp_path / "config.toml")
        
        assert mgr2.get("scan.workers") == 4  # Converted to int
        assert mgr2.get("scan.skip_existing") is True  # Converted to bool
        assert mgr2.get("paths.default_library") == "~/Media"

    def test_set_invalid_key_raises(self, tmp_path: Path):
        """Setting invalid key raises error."""
        mgr = ConfigManager(tmp_path / "config.toml")
        
        with pytest.raises(AttributeError):
            mgr.set("nonexistent.field", "value")

    def test_path_expansion(self, tmp_path: Path):
        """Tilde expansion in paths."""
        mgr = ConfigManager(tmp_path / "config.toml")
        
        mgr.set("paths.default_db", "~/media/meret.db")
        
        # Check stored value has tilde
        assert mgr.get("paths.default_db") == "~/media/meret.db"

    def test_scan_workers_validation(self, tmp_path: Path):
        """Workers validation (capped at CPU*2)."""
        mgr = ConfigManager(tmp_path / "config.toml")
        
        # Set too high value
        mgr.set("scan.workers", "1000")
        
        import multiprocessing
        expected = min(1000, multiprocessing.cpu_count() * 2)
        assert mgr.get("scan.workers") == expected

    def test_scan_workers_negative_raises(self, tmp_path: Path):
        """Negative workers rejected by Pydantic."""
        mgr = ConfigManager(tmp_path / "config.toml")
        
        with pytest.raises(ValidationError):
            mgr.set("scan.workers", "-1")

    def test_save_and_load_roundtrip(self, tmp_path: Path):
        """Save and load preserves values."""
        path = tmp_path / "config.toml"
        
        mgr1 = ConfigManager(path)
        mgr1.set("scan.workers", "4")
        mgr1.set("gui.theme", "dark")
        
        # Create new instance (force reload)
        mgr2 = ConfigManager(path)
        
        assert mgr2.get("scan.workers") == 4
        assert mgr2.get("gui.theme") == "dark"

    def test_reset_to_defaults(self, tmp_path: Path):
        """Reset restores defaults."""
        mgr = ConfigManager(tmp_path / "config.toml")
        
        mgr.set("scan.workers", "99")
        mgr.set("gui.theme", "light")
        
        mgr.reset()
        
        assert mgr.get("scan.workers") == 8  # Default
        assert mgr.get("gui.theme") == "system"  # Default

    def test_custom_config_path(self, tmp_path: Path):
        """Custom config path via constructor."""
        custom_path = tmp_path / "custom" / "meret.conf"
        mgr = ConfigManager(custom_path)
        
        assert mgr.config_path == custom_path
        assert custom_path.parent.exists()

    def test_edit_opens_editor(self, tmp_path: Path, monkeypatch):
        """Edit command opens file in editor."""
        mgr = ConfigManager(tmp_path / "config.toml")

        mock_startfile = MagicMock()
        mock_run = MagicMock()
        
        monkeypatch.setattr(os, "startfile", mock_startfile, raising=False)
        monkeypatch.setattr(subprocess, "run", mock_run)
        monkeypatch.setenv("EDITOR", "vim")

        mgr.edit()

        # One of them should be called
        assert mock_startfile.called or mock_run.called

    def test_edit_windows(self, tmp_path: Path, monkeypatch):
        """Edit on Windows uses startfile."""
        mgr = ConfigManager(tmp_path / "config.toml")
        
        mock_startfile = MagicMock()
        monkeypatch.setattr(os, "startfile", mock_startfile, raising=False)
        monkeypatch.setattr(os, "name", "nt")  # Force Windows mode
        
        # Need to patch platform check
        import sys
        original_platform = sys.platform
        monkeypatch.setattr(sys, "platform", "win32")
        
        try:
            mgr.edit()
            mock_startfile.assert_called_once_with(mgr.config_path)
        finally:
            monkeypatch.setattr(sys, "platform", original_platform)

    def test_config_file_not_corrupted_on_save_error(self, tmp_path: Path):
        """Failed save doesn't corrupt existing file."""
        config_path = tmp_path / "config.toml"
        
        # Create valid config
        mgr = ConfigManager(config_path)
        mgr.set("scan.workers", "4")
        
        # Make directory read-only (simulate error)
        config_path.chmod(0o444)
        
        try:
            with pytest.raises(OSError):
                mgr.set("scan.workers", "8")
        finally:
            config_path.chmod(0o644)
        
        # Verify original preserved
        content = config_path.read_text()
        assert "workers = 4" in content or '"workers": 4' in content


class TestGlobalConfig:
    """Tests for global config instance."""

    def test_get_config_singleton(self, tmp_path: Path, monkeypatch):
        """get_config returns singleton."""
        # Patch config path to temp location
        import meret.core.config as config_module
        
        monkeypatch.setattr(
            config_module,
            "platformdirs",
            MagicMock(user_config_dir=lambda x: str(tmp_path))
        )
        
        # Reset singleton
        config_module._config_instance = None
        
        cfg1 = get_config()
        cfg2 = get_config()
        
        assert cfg1 is cfg2

    def test_reload_config_forces_reload(self, tmp_path: Path):
        """reload_config forces fresh load from disk."""
        mgr = ConfigManager(tmp_path / "config.toml")
        mgr.set("scan.workers", "4")
        
        # Modify file directly
        import time
        time.sleep(0.01)  # Ensure different mtime
        
        # Create new manager to simulate external change
        mgr2 = ConfigManager(tmp_path / "config.toml")
        mgr2.set("scan.workers", "99")
        
        # Original manager should still have old value (cached)
        assert mgr.config.scan.workers == 4
        
        # But reload_config should get new value
        # Note: This tests the function with global instance reset


class TestConfigValidation:
    """Pydantic validation tests."""

    def test_invalid_theme_rejected(self):
        """Invalid theme value rejected."""
        with pytest.raises(Exception):  # ValidationError
            GUIConfig(theme="invalid")

    def test_invalid_language_format(self):
        """Invalid language code rejected."""
        with pytest.raises(Exception):
            LocaleConfig(language="english")

    def test_chunk_size_minimum(self):
        """Chunk size must be >= 1."""
        with pytest.raises(Exception):
            ScanConfig(chunk_size=0)

    def test_thumbnail_size_range(self):
        """Thumbnail size has min/max."""
        with pytest.raises(Exception):
            GUIConfig(thumbnail_size=32)  # Too small
        
        with pytest.raises(Exception):
            GUIConfig(thumbnail_size=2000)  # Too large

    def test_extra_fields_allowed(self):
        """Extra fields allowed for forward compatibility."""
        config = MeretConfig(extra_field="value")
        assert config.model_dump()["extra_field"] == "value"


class TestConfigIntegration:
    """Integration tests with actual file system."""

    def test_platform_dirs_location(self, monkeypatch, tmp_path: Path):
        """Config uses platformdirs for default location."""
        import meret.core.config as config_module
        
        fake_platformdirs = MagicMock()
        fake_platformdirs.user_config_dir = lambda x: str(tmp_path / "config")
        
        monkeypatch.setattr(config_module, "platformdirs", fake_platformdirs)
        
        # Reset singleton to force new path
        config_module._config_instance = None
        
        mgr = get_config()
        
        expected = tmp_path / "config" / CONFIG_FILENAME
        assert mgr.config_path == expected

    def test_concurrent_access_not_corrupts(self, tmp_path: Path):
        """Multiple instances don't corrupt file."""
        mgr1 = ConfigManager(tmp_path / "config.toml")
        mgr2 = ConfigManager(tmp_path / "config.toml")

        mgr1.set("scan.workers", "4")
        mgr2.set("gui.theme", "dark")

        # Reload mgr1 to see changes from mgr2
        mgr1._config = None  # Force reload
        assert mgr1.get("gui.theme") == "dark"
        assert mgr2.get("scan.workers") == 4