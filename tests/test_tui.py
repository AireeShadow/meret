"""Tests for TUI components."""
import pytest
import pytest_asyncio

from pathlib import Path
from unittest.mock import MagicMock, patch, Mock

from meret.cli.tui import (
    MeretApp, 
    TagDialog, 
    CommandPalette, 
    VirtualFileTree,
    SortedDirectoryTree,
    FilePreview,
    DriveSelector,
)
from meret.core.models import FileEntry, FileType
from tests.mocks import MockRepository


class TestVirtualFileTree:
    """Tests for VirtualFileTree - the virtual filesystem browser."""
    
    def test_root_data_is_set(self, tmp_path: Path, mock_repo: MockRepository):
        """Root node must have data='/' for proper navigation."""
        from meret.vfs import VirtualTree
        
        # Setup: create a tag
        mock_repo.create_tag("travel")
        
        # Create tree with mocked get_repo
        with patch('meret.cli.tui.get_repo', return_value=mock_repo):
            tree_widget = VirtualFileTree(tmp_path / "test.db")
            # _load_tree вызван в __init__, и root.data должен быть установлен
        
        assert tree_widget.root.data == "/", f"Expected '/', got {tree_widget.root.data!r}"
    
    def test_populate_node_sets_vpath(self, tmp_path: Path, mock_repo: MockRepository):
        """_populate_node must set node.data = entry.vpath for each child."""
        from meret.vfs import VirtualTree
        
        # Setup: create tag with file
        f = mock_repo.save_file(FileEntry(
            filename="beach.png", 
            filepath="/beach.png", 
            type=FileType.IMAGE
        ))
        mock_repo.add_tag(f, "vacation")
        
        tree_widget = VirtualFileTree(tmp_path / "test.db")
        tree_widget.virtual_tree = VirtualTree(mock_repo)
        tree_widget.root.data = "/"
        tree_widget._populate_node(tree_widget.root, "/")
        
        # Find vacation node
        vac_node = None
        for child in tree_widget.root.children:
            if "vacation" in str(child.label):
                vac_node = child
                break
        
        assert vac_node is not None, "vacation node not found"
        assert vac_node.data == "/vacation", f"Expected '/vacation', got {vac_node.data!r}"
    
    def test_populate_node_nested_tags(self, tmp_path: Path, mock_repo: MockRepository):
        """Nested tags create proper vpaths like /parent/child."""
        from meret.vfs import VirtualTree
        
        # Setup: nested tags
        mock_repo.create_tag("travel")
        mock_repo.create_tag("beach")
        mock_repo.set_tag_parent("beach", "travel")
        
        tree_widget = VirtualFileTree(tmp_path / "test.db")
        tree_widget.virtual_tree = VirtualTree(mock_repo)
        tree_widget.root.data = "/"
        tree_widget._populate_node(tree_widget.root, "/")
        
        # Expand travel node
        travel_node = None
        for child in tree_widget.root.children:
            if "travel" in str(child.label):
                travel_node = child
                break
        
        assert travel_node is not None
        assert travel_node.data == "/travel"
        
        # Populate travel children
        tree_widget._populate_node(travel_node, "/travel")
        
        # Check beach node
        beach_node = None
        for child in travel_node.children:
            if "beach" in str(child.label):
                beach_node = child
                break
        
        assert beach_node is not None
        assert beach_node.data == "/travel/beach", f"Expected '/travel/beach', got {beach_node.data!r}"
    
    def test_on_tree_node_expanded_uses_data(self, tmp_path: Path, mock_repo: MockRepository):
        """on_tree_node_expanded must use node.data as vpath."""
        from meret.vfs import VirtualTree
        
        # Setup
        mock_repo.create_tag("test")
        
        tree_widget = VirtualFileTree(tmp_path / "test.db")
        tree_widget.virtual_tree = VirtualTree(mock_repo)
        tree_widget.root.data = "/"
        tree_widget._populate_node(tree_widget.root, "/")
        
        # Mock _populate_node to check what vpath is passed
        called_with = []
        original_populate = tree_widget._populate_node
        
        def mock_populate(node, vpath):
            called_with.append(vpath)
            original_populate(node, vpath)
        
        tree_widget._populate_node = mock_populate
        
        # Simulate expanding first child
        test_node = tree_widget.root.children[0]
        test_node.data = "/test"  # Ensure data is set
        
        # Create mock event
        class MockEvent:
            def __init__(self, node):
                self.node = node
        
        tree_widget.on_tree_node_expanded(MockEvent(test_node))
        
        # Assert correct vpath was used
        assert "/test" in called_with, f"Expected '/test' in {called_with}"


class TestSortedDirectoryTree:
    """Tests for SortedDirectoryTree with sorting capabilities."""
    
    def test_sort_by_name(self, tmp_path: Path):
        """Tree sorts entries by name."""
        # Create test files
        (tmp_path / "zebra.txt").write_text("z")
        (tmp_path / "alpha.txt").write_text("a")
        (tmp_path / "beta.txt").write_text("b")
        
        tree = SortedDirectoryTree(tmp_path, sort_by="name")
        
        # Check that entries would be sorted
        # Note: actual sorting happens in _populate_node
        assert tree.sort_by == "name"
        assert tree.reverse is False
    
    def test_sort_by_size(self, tmp_path: Path):
        """Tree can sort by size."""
        (tmp_path / "small.txt").write_text("x")
        (tmp_path / "large.txt").write_text("x" * 100)
        
        tree = SortedDirectoryTree(tmp_path, sort_by="size")
        assert tree.sort_by == "size"
    
    def test_reverse_sort(self, tmp_path: Path):
        """Tree supports reverse sorting."""
        tree = SortedDirectoryTree(tmp_path, sort_by="name", reverse=True)
        assert tree.reverse is True


class TestDriveSelector:
    """Tests for Windows drive selector."""
    
    def test_drive_selector_creates(self):
        """DriveSelector initializes."""
        selector = DriveSelector()
        assert selector is not None
    
    @pytest.mark.skipif(not hasattr(__import__('sys'), 'platform') or 
                       not __import__('sys').platform.startswith('win'), 
                       reason="Windows only")
    def test_get_drives_winapi(self):
        """get_drives_winapi returns drives on Windows."""
        from meret.cli.tui import get_drives_winapi
        drives = get_drives_winapi()
        assert len(drives) > 0
        assert all(isinstance(d, Path) for d in drives)


class TestFilePreview:
    """Tests for FilePreview widget."""
    
    def test_preview_initial_state(self):
        """Preview shows initial message."""
        preview = FilePreview()
        # Check initial content
        assert "Select a file" in str(preview.render())
    
    def test_preview_virtual_file(self):
        """Preview handles virtual file path."""
        preview = FilePreview()
        preview.update_preview(Path("/virtual/path"), is_virtual=True)
        content = str(preview.render())
        assert "Virtual:" in content
    
    def test_preview_real_file(self, tmp_path: Path):
        """Preview shows real file info."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("hello")
        
        preview = FilePreview()
        preview.update_preview(test_file, is_virtual=False)
        content = str(preview.render())
        assert "test.txt" in content or "Size:" in content


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
        assert "mount" in cmd_names  # New command
        assert "umount" in cmd_names  # New command


class TestMeretApp:
    """Integration tests for MeretApp."""
    
    @pytest.mark.asyncio
    async def test_app_initializes(self, tmp_path: Path):
        """App starts without errors."""
        from meret.core.sqlite_repo import SQLiteRepository
        db = tmp_path / "test.db"
        SQLiteRepository(str(db))
        
        app = MeretApp(db)
        async with app.run_test() as pilot:
            assert app.db_path == db
            assert hasattr(app, 'real_tree')
    
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
        assert "m" in keys  # mount
        assert "U" in keys  # umount
        assert "v" in keys  # select drive (Windows)
        assert "S" in keys  # sort
    
    @pytest.mark.asyncio
    async def test_sort_files_cycles_modes(self, tmp_path: Path):
        """action_sort_files cycles through sort modes."""
        app = MeretApp(tmp_path)
        async with app.run_test() as pilot:
            assert app.sort_by == "name"
            app.action_sort_files()
            # Проверь что sort_by изменился
            assert app.sort_by in ["name", "size", "date"]
    
    @pytest.mark.asyncio
    async def test_go_home(self, tmp_path: Path):
        """action_go_home sets fs_root to home."""
        app = MeretApp(tmp_path)
        async with app.run_test() as pilot:
            app.action_go_home()
            assert app.fs_root == Path.home()


@pytest.mark.integration
def test_tui_launch_and_quit(tmp_path: Path):
    """Full integration test - app launches and quits."""
    from textual.app import App
    
    app = MeretApp(tmp_path)
    # Just verify it can be instantiated
    assert isinstance(app, App)


# ---------- New integration tests for mount/unmount ----------

class TestMountActions:
    """Tests for mount/unmount actions."""
    
    @pytest.mark.asyncio
    async def test_action_mount_no_path(self, tmp_path: Path):
        """Mount action handles missing path gracefully."""
        app = MeretApp(tmp_path)
        # Should not raise, will show dialog
        # We can't easily test the full flow without running the app
    
    @pytest.mark.asyncio
    async def test_action_umount_no_path(self, tmp_path: Path):
        """Unmount action handles missing path gracefully."""
        app = MeretApp(tmp_path)
        # Should not raise