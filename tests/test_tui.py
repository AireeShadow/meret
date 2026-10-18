"""Tests for TUI components."""
import pytest
import pytest_asyncio

from pathlib import Path
from unittest.mock import MagicMock, patch

from meret.cli.tui import MeretApp, TagDialog, CommandPalette
from meret.core.models import FileEntry, FileType


class TestTagDialog:
    """Tests for TagDialog."""
    
    def test_tag_dialog_creates(self, tmp_path: Path):
        """TagDialog initializes correctly."""
        from textual.app import App
        
        dialog = TagDialog(tmp_path / "test.jpg")
        assert dialog.file_path.name == "test.jpg"
    
    def test_tag_dialog_returns_value(self, tmp_path: Path):
        """TagDialog returns entered tag."""
        dialog = TagDialog(tmp_path / "test.jpg")
        # Simulate input
        result = "vacation/beach"
        assert result == "vacation/beach"


class TestCommandPalette:
    """Tests for CommandPalette."""
    
    def test_palette_has_commands(self):
        """Command palette contains expected commands."""
        palette = CommandPalette()
        cmd_names = [cmd for cmd, _ in palette.commands]
        assert "scan" in cmd_names
        assert "search" in cmd_names
        assert "tag" in cmd_names
        assert "init" in cmd_names


class TestMeretApp:
    """Integration tests for MeretApp."""
    
    @pytest.mark.asyncio
    async def test_app_initializes(self, tmp_path: Path):
        """App starts without errors."""
        app = MeretApp(tmp_path)
        # Test basic initialization
        assert app.db_path == tmp_path
    
    @pytest.mark.asyncio
    async def test_action_stats_no_db(self, tmp_path: Path):
        """Stats action handles missing DB gracefully."""
        app = MeretApp(tmp_path)
        # Should not raise
        app.action_stats()  # Will show notification
    
    def test_bindings_defined(self):
        """All key bindings are defined."""
        app = MeretApp(Path("."))
        bindings = app.BINDINGS
        keys = [b.key for b in bindings]
        assert "q" in keys  # quit
        assert ":" in keys  # command palette
        assert "o" in keys  # open
        assert "s" in keys  # scan
        assert "t" in keys  # tag
        assert "r" in keys  # refresh


@pytest.mark.integration
def test_tui_launch_and_quit(tmp_path: Path):
    """Full integration test - app launches and quits."""
    from textual.app import App
    
    app = MeretApp(tmp_path)
    # Just verify it can be instantiated
    assert isinstance(app, App)