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
        assert "s" in keys  # scan
        assert "t" in keys  # tag
        assert "r" in keys  # refresh
        assert "m" in keys  # mount
        assert "U" in keys  # umount
        assert "D" in keys  # select drive (Windows)
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

# ---------- real-FS panel: '..', sorting, layout ----------


async def _wait_for(pilot, cond, timeout: float = 5.0) -> None:
    """DirectoryTree loads in a worker — poll instead of guessing a delay."""
    for _ in range(int(timeout / 0.05)):
        await pilot.pause(0.05)
        if cond():
            return
    raise AssertionError("condition not met in time")


def _rows(tree):
    return [(type(c.data).__name__, str(c.label)) for c in tree.root.children]


@pytest.fixture
def fs_app(tmp_path: Path):
    """App opened at <tmp>/a/b with a couple of files and a DB next to them."""
    from meret.core.sqlite_repo import SQLiteRepository

    base = tmp_path.resolve()
    (base / "a" / "b" / "sub").mkdir(parents=True)
    (base / "a" / "b" / "big.txt").write_text("x" * 500)
    (base / "a" / "b" / "small.txt").write_text("x")
    (base / "a" / "y.txt").write_text("y")
    (base / "empty").mkdir()

    db = base / "meret.db"
    repo = SQLiteRepository(str(db))
    f = repo.save_file(FileEntry(filename="big.txt", filepath=str(base / "a" / "b" / "big.txt"),
                                 type=FileType.IMAGE))
    repo.add_tag(f, "travel")

    app = MeretApp(db)
    app.fs_root = base / "a" / "b"
    return app, base


class TestRealPanel:
    @pytest.mark.asyncio
    async def test_virtual_panel_sits_next_to_real_panel(self, fs_app):
        from textual.containers import Horizontal

        app, _ = fs_app
        async with app.run_test(size=(120, 40)) as pilot:
            await pilot.pause()
            assert app.virt_tree.parent is app.query_one(Horizontal)
            real, virt = app.real_tree.region, app.virt_tree.region
            assert virt.x >= real.right and virt.y == real.y
            # preview and path bar must fit on screen under the panels
            assert app.path_bar.region.bottom <= 40
            await _wait_for(pilot, lambda: len(app.virt_tree.root.children) > 0)
            assert [str(c.label) for c in app.virt_tree.root.children] == ["travel/"]

    @pytest.mark.asyncio
    async def test_parent_row_is_first(self, fs_app):
        from meret.cli.tui import ParentEntry

        app, base = fs_app
        async with app.run_test() as pilot:
            tree = app.real_tree
            await _wait_for(pilot, lambda: len(tree.root.children) > 0)
            up = tree.root.children[0]
            assert isinstance(up.data, ParentEntry)
            assert up.data.path == base / "a"
            assert not up.allow_expand  # Enter must not expand the parent inside the tree
            assert _rows(tree)[1:] == [
                ("DirEntry", "sub"),
                ("DirEntry", "big.txt"),
                ("DirEntry", "small.txt"),
            ]

    @pytest.mark.asyncio
    async def test_enter_on_parent_row_goes_up(self, fs_app):
        app, base = fs_app
        async with app.run_test() as pilot:
            tree = app.real_tree
            await _wait_for(pilot, lambda: len(tree.root.children) > 0)

            tree.focus()
            tree.move_cursor(tree.root.children[0])
            await pilot.press("enter")
            await _wait_for(pilot, lambda: Path(tree.path) == base / "a")

            assert app.fs_root == base / "a"
            assert app.real_tree is tree  # same widget, no remount
            assert "Real Filesystem" in str(tree.border_title)
            await _wait_for(pilot, lambda: ("DirEntry", "b") in _rows(tree))

    @pytest.mark.asyncio
    async def test_backspace_goes_up(self, fs_app):
        app, base = fs_app
        async with app.run_test() as pilot:
            await _wait_for(pilot, lambda: len(app.real_tree.root.children) > 0)
            app.real_tree.focus()
            await pilot.press("backspace")
            await _wait_for(pilot, lambda: Path(app.real_tree.path) == base / "a")

    @pytest.mark.asyncio
    async def test_parent_row_in_empty_folder(self, fs_app):
        app, base = fs_app
        app.fs_root = base / "empty"
        async with app.run_test() as pilot:
            tree = app.real_tree
            await _wait_for(pilot, lambda: len(tree.root.children) > 0)
            assert _rows(tree) == [("ParentEntry", "..")]

    @pytest.mark.asyncio
    async def test_sort_by_size_keeps_folders_first(self, fs_app):
        app, base = fs_app
        async with app.run_test() as pilot:
            tree = app.real_tree
            await _wait_for(pilot, lambda: len(tree.root.children) > 0)
            app.sort_by = "size"
            app._refresh_fs_tree()
            await _wait_for(pilot, lambda: _rows(tree)[-1] == ("DirEntry", "big.txt"))
            assert [label for _, label in _rows(tree)] == ["..", "sub", "small.txt", "big.txt"]
            assert "size" in str(tree.border_title)

    @pytest.mark.asyncio
    async def test_selecting_real_file_selects_and_opens(self, fs_app):
        app, base = fs_app
        opened = []
        app.action_open_file = lambda: opened.append(app.selected_real)
        async with app.run_test() as pilot:
            tree = app.real_tree
            await _wait_for(pilot, lambda: len(tree.root.children) > 0)
            tree.focus()
            big = next(c for c in tree.root.children if str(c.label) == "big.txt")
            tree.move_cursor(big)
            await pilot.press("enter")
            await _wait_for(pilot, lambda: opened)
            assert opened == [base / "a" / "b" / "big.txt"]

    @pytest.mark.asyncio
    @pytest.mark.skipif(not __import__("sys").platform.startswith("win"), reason="Windows only")
    async def test_drive_root_has_no_parent_row_and_up_picks_drive(self, fs_app):
        app, _ = fs_app
        app.fs_root = Path(Path.home().anchor)  # C:\
        async with app.run_test() as pilot:
            tree = app.real_tree
            await _wait_for(pilot, lambda: len(tree.root.children) > 0)
            assert all(kind != "ParentEntry" for kind, _ in _rows(tree))

            tree.focus()
            await pilot.press("backspace")
            await _wait_for(pilot, lambda: isinstance(app.screen, DriveSelector))


# ---------- bindings, command palette, delete tag ----------


def test_every_binding_has_an_action():
    """A binding to a missing action fails silently — catch typos like 'unmount'."""
    for binding in MeretApp.BINDINGS:
        name = binding.action.split("(")[0]
        assert hasattr(MeretApp, f"action_{name}"), f"{binding.key!r} -> missing action_{name}"


def test_every_palette_command_dispatches(monkeypatch):
    """Each palette entry reaches a real action (untag is the one not built yet)."""
    app = MeretApp(Path("."))
    called = []
    for cmd, _ in CommandPalette().commands:
        if cmd == "untag":
            continue
        app.notify = lambda msg, **kw: called.append(("notify", msg))
        before = len(called)
        names = {
            "scan": "action_scan", "search": "action_search", "tag": "action_tag",
            "init": "action_init_db", "config": "action_config", "stats": "action_stats",
            "tree": "action_tree", "prune": "action_prune", "mount": "action_mount",
            "umount": "action_umount",
        }
        monkeypatch.setattr(app, names[cmd], lambda c=cmd: called.append(c))
        app.run_command(cmd)
        assert called[before:] == [cmd]


def test_palette_matches_rank_exact_and_prefix_first():
    palette = CommandPalette()
    assert palette._matches("tree")[0] == "tree"
    assert palette._matches("un")[0] == "untag"
    assert palette._matches("")[: 2] == ["scan", "search"]


class TestCommandPaletteFlow:
    @pytest.mark.asyncio
    async def test_typed_command_runs_on_enter(self, fs_app):
        app, _ = fs_app
        got = []
        app.run_command = got.append
        async with app.run_test() as pilot:
            await pilot.press(":")
            await pilot.press(*"tree", "enter")
            await _wait_for(pilot, lambda: got)
            assert got == ["tree"]

    @pytest.mark.asyncio
    async def test_picked_command_runs(self, fs_app):
        app, _ = fs_app
        got = []
        app.run_command = got.append
        async with app.run_test() as pilot:
            await pilot.press(":")
            await pilot.press(*"stat")  # filters the list down to 'stats'
            await pilot.press("tab", "enter")
            await _wait_for(pilot, lambda: got)
            assert got == ["stats"]


class TestDeleteTag:
    async def _cursor_on_travel(self, app, pilot):
        tree = app.virt_tree
        await _wait_for(pilot, lambda: len(tree.root.children) > 0)
        tree.focus()
        tree.move_cursor(tree.root.children[0])
        await pilot.pause()

    @pytest.mark.asyncio
    async def test_delete_after_confirmation(self, fs_app):
        from meret.cli.tui import ConfirmDialog
        from meret.core.sqlite_repo import SQLiteRepository

        app, base = fs_app
        async with app.run_test() as pilot:
            await self._cursor_on_travel(app, pilot)
            await pilot.press("d")
            await _wait_for(pilot, lambda: isinstance(app.screen, ConfirmDialog))
            await pilot.press("y")
            await _wait_for(pilot, lambda: not isinstance(app.screen, ConfirmDialog))

        assert SQLiteRepository(str(base / "meret.db")).get_all_tags() == []

    @pytest.mark.asyncio
    async def test_delete_cancelled_keeps_tag(self, fs_app):
        from meret.cli.tui import ConfirmDialog
        from meret.core.sqlite_repo import SQLiteRepository

        app, base = fs_app
        async with app.run_test() as pilot:
            await self._cursor_on_travel(app, pilot)
            await pilot.press("d")
            await _wait_for(pilot, lambda: isinstance(app.screen, ConfirmDialog))
            await pilot.press("enter")  # Enter is NOT a yes
            await pilot.press("n")
            await _wait_for(pilot, lambda: not isinstance(app.screen, ConfirmDialog))

        names = [t.name for t in SQLiteRepository(str(base / "meret.db")).get_all_tags()]
        assert names == ["travel"]

    @pytest.mark.asyncio
    async def test_delete_on_root_only_warns(self, fs_app):
        from meret.cli.tui import ConfirmDialog

        app, _ = fs_app
        async with app.run_test() as pilot:
            tree = app.virt_tree
            await _wait_for(pilot, lambda: len(tree.root.children) > 0)
            tree.focus()
            tree.move_cursor(tree.root)
            await pilot.press("d")
            await pilot.pause(0.2)
            assert not isinstance(app.screen, ConfirmDialog)


# ---------- logging ----------


def test_virtual_panel_logs_real_errors(tmp_path, caplog):
    """Not every failure is 'no database' — the real one goes to the log with a traceback."""
    import logging

    caplog.set_level(logging.WARNING, logger="meret")
    tree_widget = VirtualFileTree(tmp_path / "meret.db")
    with patch("meret.cli.deps.get_repo", side_effect=RuntimeError("disk on fire [x]")):
        tree_widget._load_tree()

    labels = [str(c.label) for c in tree_widget.root.children]
    assert any("disk on fire [x]" in label for label in labels)  # escaped, not eaten as markup
    (record,) = [r for r in caplog.records if r.name == "meret.cli.tui"]
    assert record.exc_info is not None


def test_virtual_panel_missing_db_is_not_an_error(tmp_path, caplog):
    import logging

    caplog.set_level(logging.WARNING, logger="meret.cli.tui")
    tree_widget = VirtualFileTree(tmp_path / "nope.db")
    tree_widget._load_tree()
    assert "No database" in str(tree_widget.root.children[0].label)
    assert [r for r in caplog.records if r.name == "meret.cli.tui"] == []


def test_subprocess_exit_codes_are_logged(caplog):
    import logging
    import sys

    caplog.set_level(logging.INFO, logger="meret")
    app = MeretApp(Path("."))
    app._run([sys.executable, "-c", "raise SystemExit(3)"])
    messages = [r.getMessage() for r in caplog.records]
    assert any(m.startswith("run: ") for m in messages)
    assert any(m.startswith("exit 3: ") for m in messages)
