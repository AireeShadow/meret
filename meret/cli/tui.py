"""Interactive TUI for meret (like k9s)."""

import os
import asyncio
import logging
import subprocess
import platform
import ctypes
from pathlib import Path
from typing import Optional

import logging
logger = logging.getLogger(__name__)

from rich.text import Text
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.reactive import reactive
from textual.screen import ModalScreen
from textual.widgets import (
    DataTable,
    DirectoryTree,
    Footer,
    Header,
    Input,
    Label,
    ListView,
    ListItem,
    Static,
    Tree,
    Button,
    OptionList,
)

from meret.cli.deps import get_repo, resolve_db_path, DatabaseNotFound
from meret.core.models import FileEntry, FileType
from meret.vfs import VirtualTree, NotFound

def get_drives_winapi():
    """Get drives using Windows API — instant."""
    drives = []
    buf = ctypes.create_string_buffer(260)
    ctypes.windll.kernel32.GetLogicalDriveStringsA(260, buf)
    for i in range(0, 260, 4):
        drive = buf.raw[i:i+4].decode('ascii').rstrip('\x00')
        if drive:
            drives.append(Path(drive))
    return drives

from textual.widgets import OptionList

class DriveSelector(ModalScreen[str]):
    """Simple drive selector."""
    
    def compose(self) -> ComposeResult:
        yield Label("Select Drive:")
        self.options = OptionList()
        yield self.options
    
    def on_mount(self):
        self.drives = get_drives_winapi()
        for drive in self.drives:
            self.options.add_option(str(drive))  # Just "C:\", "D:\", etc.
    
    def on_option_list_option_selected(self, event: OptionList.OptionSelected):
        # Extract drive letter from text
        text = str(event.option.prompt if hasattr(event.option, 'prompt') else event.option)
        self.dismiss(Path(text))

class FilePreview(Static):
    """Preview panel for selected file."""
    
    def __init__(self):
        super().__init__("Select a file to preview", id="preview")
        self.border_title = "Preview"
    
    def update_preview(self, path: Optional[Path], is_virtual: bool = False):
        """Update preview content."""
        if path is None:
            self.update("[dim]No selection[/dim]")
            return
        
        if is_virtual:
            self.update(f"[cyan]Virtual:[/cyan] {path}\n[dim]Use 'o' to open real file[/dim]")
        else:
            try:
                stat = path.stat()
                size = self._human_size(stat.st_size)
                self.update(f"[green]{path.name}[/green]\nSize: {size}\nModified: {stat.st_mtime}")
            except OSError:
                self.update("[red]Cannot access file[/red]")
    
    def _human_size(self, size: int) -> str:
        """Convert bytes to human readable."""
        for unit in ("B", "KB", "MB", "GB"):
            if size < 1024:
                return f"{size:.1f} {unit}"
            size /= 1024
        return f"{size:.1f} TB"


class VirtualFileTree(Tree):
    """Tree widget for virtual tag-based filesystem."""
    
    def __init__(self, db_path: Path):
        super().__init__("/", id="virtual_tree")
        self.db_path = db_path
        self.border_title = "Virtual FS (Tags)"
        self.root.data = "/"

    def on_mount(self) -> None:
        """Called when widget is mounted - safe to load data."""
        self._load_tree()
    
    def _load_tree(self):
        """Load virtual tree structure."""
        
        try:
            from meret.cli.deps import get_repo
            repo = get_repo(self.db_path)
            self.virtual_tree = VirtualTree(repo)
            self.root.expand()
            self._populate_node(self.root, "/")
        except Exception:
            self.root.remove_children()
            self.root.add_leaf("[dim]No database - run 'meret init'[/dim]")
    
    def _populate_node(self, node, vpath: str):
        """Populate tree node with virtual directory contents."""
        try:
            entries = self.virtual_tree.listdir(vpath)
            
            # Clear existing
            node.remove_children()
            
            if not entries:
                node.add_leaf("[dim](empty)[/dim]")
                return
            
            # Add directories
            for entry in entries:
                if entry.is_dir:
                    if not entry.vpath:
                        node.add_leaf(f"[red]Error: empty vpath for {entry.name}[/red]")
                        continue
                        
                    child = node.add(entry.name + "/", data=entry.vpath)
                    child.add("[dim]...[/dim]")
                else:
                    if not entry.vpath:
                        node.add_leaf(f"[red]Error: empty vpath for {entry.name}[/red]")
                        continue
                        
                    node.add_leaf(entry.name, data=entry.vpath)
                    
        except Exception as e:
            node.add_leaf(f"[red]Error: {e}[/red]")


    def on_tree_node_expanded(self, event: Tree.NodeExpanded):
        """Lazy load children when node expanded."""
        node = event.node
        logger.debug(f"Expanding node: label={node.label}, data={node.data}")
        
        vpath = node.data
        if not vpath:
            self.notify(f"Error: no vpath for node {node.label}", severity="error")
            return
        
        needs_load = (
            not node.children 
            or (len(node.children) == 1 and "..." in str(node.children[0].label))
        )
        
        if needs_load:
            node.remove_children()
            self._populate_node(node, vpath)


class TagDialog(ModalScreen[str]):
    """Dialog for entering tag name."""
    
    BINDINGS = [
        Binding("escape", "dismiss", "Cancel"),
        Binding("enter", "submit", "OK"),
    ]
    
    def __init__(self, file_path: Path):
        super().__init__()
        self.file_path = file_path
    
    def compose(self) -> ComposeResult:
        yield Label(f"Tag file: {self.file_path.name}")
        yield Input(placeholder="Enter tag (e.g., vacation/beach)", id="tag_input")
        yield Label("[dim]Press Enter to confirm, Esc to cancel[/dim]")
    
    def on_input_submitted(self, event: Input.Submitted):
        self.dismiss(event.value)
    
    def action_submit(self):
        inp = self.query_one("#tag_input", Input)
        self.dismiss(inp.value)


class SearchScreen(ModalScreen[list[FileEntry]]):
    """Search results screen."""
    
    BINDINGS = [
        Binding("escape", "dismiss", "Close"),
        Binding("enter", "select", "Select"),
    ]
    
    def __init__(self, repo, query: str):
        super().__init__()
        self.repo = repo
        self.query = query
        self.results: list[FileEntry] = []
    
    def compose(self) -> ComposeResult:
        yield Label(f"Search: {self.query}")
        self.list_view = ListView(id="results")
        yield self.list_view
        yield Label("[dim]↑↓ to navigate, Enter to select, Esc to close[/dim]")
    
    def on_mount(self):
        """Perform search."""
        try:
            if self.query.startswith("tag:"):
                tag = self.query[4:]
                self.results = self.repo.search_by_tags([tag])
            else:
                self.results = self.repo.search_by_name(self.query)
            
            for f in self.results[:50]:
                self.list_view.append(ListItem(Label(f"{f.filename} [{f.type.value}]")))
        except Exception as e:
            self.list_view.append(ListItem(Label(f"[red]Error: {e}[/red]")))
    
    def on_list_view_selected(self, event: ListView.Selected):
        idx = event.list_view.index
        if 0 <= idx < len(self.results):
            self.dismiss(self.results[idx])


class InputDialog(ModalScreen[str]):
    """Generic input dialog."""
    
    def __init__(self, prompt: str):
        super().__init__()
        self.prompt = prompt
    
    def compose(self) -> ComposeResult:
        yield Label(self.prompt)
        yield Input(id="input")
    
    def on_input_submitted(self, event: Input.Submitted):
        self.dismiss(event.value)
    
    def on_key(self, event):
        if event.key == "escape":
            self.dismiss(None)


class CommandPalette(ModalScreen[str]):
    """Command palette like in k9s."""
    
    BINDINGS = [
        Binding("escape", "dismiss", "Close"),
        Binding("enter", "select", "Select"),
    ]
    
    def __init__(self):
        super().__init__()
        self.commands = [
            ("scan", "Scan directory for media"),
            ("search", "Search files by tags/name"),
            ("tag", "Tag selected file"),
            ("untag", "Remove tag from file"),
            ("init", "Initialize new database"),
            ("config", "Edit configuration"),
            ("stats", "Show database statistics"),
            ("tree", "Show tag tree"),
            ("prune", "Remove orphaned tags"),
            ("mount", "Mount virtual filesystem"),
            ("umount", "Unmount filesystem"),
        ]
    
    def compose(self) -> ComposeResult:
        yield Input(placeholder="Type command...", id="cmd_input")
        yield ListView(
            *[ListItem(Label(f"{cmd:<15} {desc}")) for cmd, desc in self.commands],
            id="cmd_list"
        )
    
    def on_input_changed(self, event: Input.Changed):
        """Filter commands based on input."""
        list_view = self.query_one("#cmd_list", ListView)
        list_view.clear()
        query = event.value.lower()
        for cmd, desc in self.commands:
            if query in cmd.lower() or query in desc.lower():
                list_view.append(ListItem(Label(f"{cmd:<15} {desc}")))
    
    def on_list_view_selected(self, event: ListView.Selected):
        """Handle command selection."""
        label = str(event.item.children[0])
        cmd = label.split()[0]
        self.dismiss(cmd)


class MeretApp(App):
    """Main TUI application for meret."""
    
    CSS = """
    Screen { align: center middle; }
    
    Horizontal {
        width: 100%;
        height: 100%;
    }
    
    DirectoryTree {
        width: 50%;
        border: solid green;
    }
    
    VirtualFileTree {
        width: 50%;
        border: solid cyan;
    }
    
    FilePreview {
        height: 20%;
        border: solid yellow;
    }
    
    #path_bar {
        height: 1;
        background: $surface;
        color: $text;
        content-align: center middle;
    }
    
    CommandPalette {
        align: center middle;
        width: 60;
        height: 20;
        border: solid blue;
        background: $surface;
    }
    
    Input {
        margin: 1;
    }
    
    ListView {
        margin: 1;
    }
    
    Footer {
        height: 1;
    }
    """
    
    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding(":", "command_palette", "Command"),
        Binding("i", "init_db", "Init DB"),
        Binding("enter", "open_file", "Open"),
        Binding("s", "scan", "Scan"),
        Binding("t", "tag", "Tag"),
        Binding("r", "refresh", "Refresh"),
        Binding("c", "config", "Config"),
        Binding("d", "delete", "Delete tag"),
        Binding("h", "go_home", "Home"),
        Binding("~", "go_home", "Home"),
        Binding("u", "go_up", "Up"),
        Binding("backspace", "go_up", "Up"),
        Binding("S", "sort_files", "Sort"),
        Binding("m", "mount", "Mount"),
        Binding("U", "unmount", "unmount"),
        Binding("D", "select_drive", "Select Drive"),  # Windows only
        Binding("/", "search", "Search"),
        Binding("?", "help", "Help"),
    ]
    
    def __init__(self, db_path: Optional[Path] = None):
        super().__init__()
        self.db_path = db_path or Path(".")
        self.selected_real: Optional[Path] = None
        self.selected_virtual: Optional[str] = None
        # Start from home directory for full filesystem access
        self.fs_root = Path.home()
        self.sort_by = "name"  # name, size, date
        self.sort_reverse = False
    
    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        
        with Horizontal():
            # Left panel: Real filesystem - start from home for full navigation
            self.real_tree = DirectoryTree(self.fs_root)
            self.real_tree.border_title = f"Real Filesystem ({self.fs_root})"
            yield self.real_tree
            
            # Right panel: Virtual filesystem
            self.virt_tree = VirtualFileTree(self.db_path)
            yield self.virt_tree
        
        # Bottom panel: Preview/info
        self.preview = FilePreview()
        yield self.preview
        
        # Path bar showing current selection
        self.path_bar = Static(f"DB: {self.db_path} | Selected: -", id="path_bar")
        yield self.path_bar
        
        yield Footer()
    
    def on_directory_tree_file_selected(self, event: DirectoryTree.FileSelected):
        """Handle real file selection."""
        if '..' in str(event.node.label):
            self.action_go_up()
            return
        self.selected_real = event.path
        self.selected_virtual = None
        self.preview.update_preview(event.path, is_virtual=False)
        self._update_path_bar()
    
    def on_directory_tree_directory_selected(self, event: DirectoryTree.DirectorySelected):
        """Handle real directory selection."""
        # Check for '..' by label
        if '..' in str(event.node.label) or '⬆️' in str(event.node.label):
            self.action_go_up()
            return
        
        # Regular directory
        self.selected_real = event.path
        self.selected_virtual = None
        self.preview.update_preview(event.path, is_virtual=False)
        self._update_path_bar()
    
    def _update_path_bar(self):
        """Update path display."""
        sel = str(self.selected_real) if self.selected_real else "-"
        self.path_bar.update(f"DB: {self.db_path} | Selected: {sel}")
    
    def on_tree_node_selected(self, event: Tree.NodeSelected):
        """Handle virtual file selection - ALSO open on Enter."""
        if hasattr(event.node, 'data') and event.node.data:
            self.selected_real = None
            self.selected_virtual = event.node.data
            self.preview.update_preview(Path(event.node.data), is_virtual=True)
            
            node_label = str(event.node.label)
            if not event.node.allow_expand and not node_label.endswith("/"):
                self.action_open_file()

    def on_directory_tree_file_selected(self, event: DirectoryTree.FileSelected):
        """Handle real file selection."""
        if event.path.name == '..' or (hasattr(self, '_up_selected') and self._up_selected):
            self._up_selected = False
            self.action_go_up()
        return
        self.selected_real = event.path
        self.selected_virtual = None
        self.preview.update_preview(event.path, is_virtual=False)
        self._update_path_bar()
        
        self.action_open_file()

    def action_select_or_open(self):
        """Enter on file opens it, on dir expands it."""
        if self.focused == self.real_tree:
            if self.selected_real and self.selected_real.is_file():
                self.action_open_file()
            else:
                self.real_tree.action_select_cursor()
        elif self.focused == self.virt_tree:
            if self.selected_virtual:
                try:
                    from meret.vfs import VirtualTree
                    repo = get_repo(self.db_path)
                    tree = VirtualTree(repo)
                    node = tree.stat(self.selected_virtual)
                    if node.is_dir:
                        self.virt_tree.action_select_cursor()
                    else:
                        self.action_open_file()
                except:
                    pass
    
    def action_command_palette(self):
        """Open command palette."""
        def on_command(cmd: str | None):
            if cmd:
                self.run_command(cmd)
        
        self.push_screen(CommandPalette(), on_command)
    
    def run_command(self, cmd: str):
        """Execute meret command."""
        if cmd == "scan":
            self.action_scan()
        elif cmd == "search":
            self.action_search()
        elif cmd == "stats":
            self.action_stats()
        elif cmd == "tree":
            self.action_tree()
        elif cmd == "config":
            self.action_config()
    
    def action_scan(self):
        """Scan current directory."""
        if self.selected_real and self.selected_real.is_dir():
            self.notify(f"Scanning {self.selected_real}...")
            subprocess.run(["meret", "scan", str(self.selected_real), "--db", str(self.db_path)])
            self.virt_tree._load_tree()
            self.notify("Scan complete!")
    
    def action_open_file(self):
        """Open selected file with default app."""
        if self.selected_real and self.selected_real.is_file():
            if os.name == 'nt':
                os.startfile(self.selected_real)
            else:
                subprocess.run(["xdg-open", str(self.selected_real)])
            self.notify(f"Opened {self.selected_real.name}")
            
        elif self.selected_virtual:
            try:
                from meret.vfs import VirtualTree
                
                repo = get_repo(self.db_path)
                tree = VirtualTree(repo)
                real_path = tree.real_path(self.selected_virtual)
                
                if os.name == 'nt':
                    os.startfile(real_path)
                else:
                    subprocess.run(["xdg-open", str(real_path)])
                    
                self.notify(f"Opened {self.selected_virtual} → {real_path.name}")
                
            except Exception as e:
                self.notify(f"Error opening: {e}", severity="error")
    
    def action_tag(self):
        """Tag selected file."""
        if not self.selected_real or not self.selected_real.is_file():
            self.notify("Select a real file first!", severity="error")
            return
        
        def on_tag(tag: str | None):
            if tag:
                try:
                    repo = get_repo(self.db_path)
                    entry = repo.get_file_by_path(str(self.selected_real))
                    if not entry:
                        from meret.core.models import FileEntry, FileType
                        entry = FileEntry(
                            filename=self.selected_real.name,
                            filepath=str(self.selected_real),
                            type=FileType.UNKNOWN
                        )
                        file_id = repo.save_file(entry)
                    else:
                        file_id = entry.id
                    
                    repo.add_tag(file_id, tag)
                    self.notify(f"Tagged with '{tag}'")
                    self.virt_tree._load_tree()
                    
                except Exception as e:
                    self.notify(f"Error: {e}", severity="error")
        
        self.push_screen(TagDialog(self.selected_real), on_tag)
    
    def action_search(self):
        """Search dialog."""
        def on_query(query: str | None):
            if query:
                def on_result(result: FileEntry | None):
                    if result:
                        self.notify(f"Selected: {result.filename}")
                
                try:
                    repo = get_repo(self.db_path)
                    self.push_screen(SearchScreen(repo, query), on_result)
                except DatabaseNotFound:
                    self.notify("No database!", severity="error")
        
        self.push_screen(InputDialog("Search (name or 'tag:xxx'):"), on_query)
    
    def action_stats(self):
        """Show database stats."""
        try:
            repo = get_repo(self.db_path)
            stats = repo.get_stats()
            self.notify(f"Files: {stats['total_files']}, Tags: {stats['total_tags']}")
        except DatabaseNotFound:
            self.notify("No database!", severity="error")
    
    def action_config(self):
        """Open config editor."""
        subprocess.run(["meret", "config", "edit"])
    
    def action_init_db(self):
        """Initialize database."""
        subprocess.run(["meret", "init", str(self.db_path)])
        self.virt_tree._load_tree()
        self.notify("Database initialized!")
    
    def action_refresh(self):
        """Refresh both panels."""
        self.real_tree.reload()
        self.virt_tree._load_tree()
        self.notify("Refreshed")
    
    def action_go_home(self):
        """Navigate to home directory."""
        self.fs_root = Path.home()
        self._refresh_fs_tree()
    
    def action_go_up(self):
        """Go to parent directory."""
        # Check if we're at Windows drive root (e.g., C:\)
        if platform.system() == "Windows":
            # If at root of drive (C:\), show drive selector
            if self.fs_root == self.fs_root.anchor:  # e.g., C:\ 
                self.action_select_drive()
                return
        
        parent = self.fs_root.parent
        if parent.exists() and str(parent) != str(self.fs_root):
            self.fs_root = parent
            self._refresh_fs_tree()

    def action_sort_files(self):
        """Toggle sort mode."""
        modes = ["name", "size", "date"]
        idx = modes.index(self.sort_by)
        self.sort_by = modes[(idx + 1) % len(modes)]
        self.notify(f"Sort by: {self.sort_by}")
        self._refresh_fs_tree()
        
    def action_select_drive(self):
        """Show drive selector (Windows only)."""
        if platform.system() != "Windows":
            self.notify("Drive selection only available on Windows", severity="warning")
            return
        
        def on_drive_selected(drive: Path | None):
            if drive:
                self.fs_root = drive
                self._refresh_fs_tree()
                self.notify(f"Switched to {drive}")
                if hasattr(self, 'real_tree'):
                    self.real_tree.focus()
        
        self.push_screen(DriveSelector(), on_drive_selected)

    def _refresh_fs_tree(self):
        """Refresh with sorting."""
        if not hasattr(self, 'real_tree'):
            return 
        # Remove old tree
        self.real_tree.remove()
        
        # Create new tree with custom sorting
        new_tree = SortedDirectoryTree(
            self.fs_root, 
            sort_by=self.sort_by,
            reverse=self.sort_reverse
        )
        new_tree.border_title = f"Real Filesystem ({self.fs_root}) [{self.sort_by}]"
        
        horizontal = self.query_one(Horizontal)
        horizontal.mount(new_tree, before=self.virt_tree)
        self.real_tree = new_tree
        self.real_tree.focus()
    
    def action_help(self):
        """Show help."""
        help_text = """
        [b]Meret TUI Help[/b]
        
        [green]Navigation:[/green]
        ↑/↓ or j/k    Navigate
        Enter         Open/Expand
        q             Quit
        
        [green]Navigation Keys:[/green]
        h or ~        Go to home directory
        u or Backspace Go to parent directory
        
        [green]Commands:[/green]
        :             Command palette
        o             Open file
        s             Scan directory
        t             Tag file
        r             Refresh
        /             Search
        c             Config
        i             Init DB
        """
        self.notify(help_text, timeout=10)

    def action_mount(self):
        """Mount virtual filesystem."""
        def on_mountpoint(path: str | None):
            if path:
                try:
                    self.notify(f"Mounting at {path}...")
                    # Import and call directly instead of subprocess
                    from meret.vfs.backends.mount import mount as do_mount
                    from meret.vfs import VirtualTree
                    
                    repo = get_repo(self.db_path)
                    tree = VirtualTree(repo)
                    
                    # Run in thread so TUI doesn't freeze
                    import threading
                    def mount_thread():
                        try:
                            do_mount(tree, Path(path))
                        except Exception as e:
                            self.call_from_thread(
                                lambda: self.notify(f"Mount error: {e}", severity="error")
                            )
                    
                    threading.Thread(target=mount_thread, daemon=True).start()
                    self.notify(f"Mounting at {path}... (Ctrl+C in console to exit)")
                    
                except ImportError as e:
                    self.notify(f"Missing driver: {e}", severity="error")
                    self.notify("Install: pip install fusepy (Linux/Mac) or winfspy (Windows)")
                except Exception as e:
                    self.notify(f"Error: {e}", severity="error")
        
        self.push_screen(InputDialog("Enter mount point (e.g., M: or /mnt/meret):"), on_mountpoint)


    def action_umount(self):
        """Unmount filesystem."""
        def on_unmount(path: str | None):
            if path:
                try:
                    subprocess.run(["meret", "umount", path], check=True)
                    self.notify(f"Unmounted {path}")
                except Exception as e:
                    self.notify(f"Error: {e}", severity="error")
        
        self.push_screen(InputDialog("Enter mount point to unmount:"), on_unmount)

class DirEntryWrapper:
    """Wrapper to make Path compatible with DirectoryTree."""
    def __init__(self, path: Path):
        self.path = path
        self.loaded = False

class SortedDirectoryTree(DirectoryTree):
    """DirectoryTree with sorting."""
    
    def __init__(self, path: Path, sort_by: str = "name", reverse: bool = False, **kwargs):
        self.sort_by = sort_by
        self.reverse = reverse
        super().__init__(path, **kwargs)
    
    async def _on_tree_node_expanded(self, event):
        """Sort after default expansion."""
        await super()._on_tree_node_expanded(event)
        self._sort_children(event.node)
    
    def _sort_node(self, node):
        """Sort children of a node."""
        if not node.children:
            return
        
        # Get (label, data) pairs
        children_data = []
        for child in node.children:
            label = str(child.label)
            data = child.data
            is_dir = label.startswith("📁")
            
            # Get sort key
            try:
                if self.sort_by == "name":
                    key = label.lower()
                elif self.sort_by == "size" and not is_dir:
                    key = Path(data).stat().st_size if data else 0
                else:
                    key = label.lower()
            except:
                key = label.lower()
            
            children_data.append((key, is_dir, label, data))
        
        # Sort: dirs first, then by key
        children_data.sort(key=lambda x: (not x[1], x[0]), reverse=self.sort_reverse)
        
        # Clear and re-add
        node.remove_children()
        for _, is_dir, label, data in children_data:
            if is_dir:
                child = node.add(label, data=data)
                child.add("[dim]...[/dim]")
            else:
                node.add_leaf(label, data=data)

    def _populate_node(self, node, content):
        """Override to add sorting and '..' entry."""
        try:
            if node == self.root:
                node_path = self.path
            else:
                node_path = node.data.path if hasattr(node.data, 'path') else Path(node.data)
            
            entries = list(node_path.iterdir())
            
            # Create list with '..' at first position
            all_entries = []
            
            # Add '..' if we can go up
            parent = node_path.parent
            if parent != node_path and str(parent) != str(node_path):
                parent_wrapper = DirEntryWrapper(parent)
                all_entries.append(('..', parent_wrapper, True))
            
            # Add regular files and folders
            for entry in entries:
                wrapper = DirEntryWrapper(entry)
                all_entries.append((entry.name, wrapper, entry.is_dir()))
            
            # Sort: first '..', then dirs, then files
            def sort_key(item):
                name, wrapper, is_dir = item
                if name == '..':
                    return (0, '')
                elif is_dir:
                    return (1, name.lower())
                else:
                    return (2, name.lower())
            
            all_entries.sort(key=sort_key, reverse=self.reverse)
            
            # Add to tree
            node.remove_children()
            
            for name, wrapper, is_dir in all_entries:
                if name == '..':
                    up_node = node.add("⬆️ ..", data=wrapper)
                    up_node.add("")
                elif is_dir:
                    child = node.add(f"📁 {name}", data=wrapper)
                    child.add("[dim]...[/dim]")
                else:
                    node.add_leaf(f"📄 {name}", data=wrapper)
                    
        except PermissionError:
            node.add_leaf("[dim]Access denied[/dim]")
        except Exception as e:
            node.add_leaf(f"[red]Error: {e}[/red]")

    def _sort_children(self, node):
        """Sort children of a node."""
        if not node.children:
            return
        
        items = []
        for child in node.children:
            label = str(child.label)
            # If child has children or starts with 📁, it's a directory
            is_dir = len(child.children) > 0 or label.startswith("📁 ")
            name = label[2:] if label.startswith(("📁 ", "📄 ")) else label
            items.append((name.lower(), is_dir, label, child.data))
        
        items.sort(key=lambda x: (not x[1], x[0]), reverse=self.reverse)
        
        node.remove_children()
        for _, is_dir, label, data in items:
            if is_dir:
                child = node.add(label, data=data)
                child.add("[dim]...[/dim]")
            else:
                node.add_leaf(label, data=data)
            
def main():
    """Entry point for TUI."""
    import sys
    db_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(".")
    app = MeretApp(db_path)
    app.run()


if __name__ == "__main__":
    main()