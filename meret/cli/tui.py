"""Interactive TUI for meret (like k9s)."""

import os
import asyncio
import logging
import subprocess
import platform
import ctypes
from pathlib import Path
from typing import Optional

from rich.markup import escape
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
from textual.widgets.directory_tree import DirEntry

from meret.cli.deps import get_repo, resolve_db_path, DatabaseNotFound
from meret.core import tagpaths
from meret.core.models import FileEntry, FileType
from meret.core.logger import log_uncaught_exceptions, setup_logging
from meret.vfs import VirtualTree, NotFound
from meret.core.locale import _

logger = logging.getLogger(__name__)

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
        self.border_title = _("Preview")
    
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
        except DatabaseNotFound:
            self.root.remove_children()
            self.root.add_leaf("[dim]No database - run 'meret init'[/dim]")
        except Exception as e:
            logger.exception("virtual panel: loading %s failed", self.db_path)
            self.root.remove_children()
            self.root.add_leaf(f"[red]Error: {escape(str(e))}[/red] [dim](meret logs)[/dim]")
    
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
            logger.exception("virtual panel: listing %s failed", vpath)
            node.add_leaf(f"[red]Error: {escape(str(e))}[/red]")


    def on_tree_node_expanded(self, event: Tree.NodeExpanded):
        """Lazy load children when node expanded."""
        node = event.node
        logger.debug("virtual panel: expanding %s", node.data)
        
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
            logger.exception("search %r failed", self.query)
            self.list_view.append(ListItem(Label(f"[red]Error: {escape(str(e))}[/red]")))
    
    def on_list_view_selected(self, event: ListView.Selected):
        idx = event.list_view.index
        if 0 <= idx < len(self.results):
            self.dismiss(self.results[idx])


class ConfirmDialog(ModalScreen[bool]):
    """Yes/no question. Only 'y' confirms: Enter is too easy to hit by accident."""

    BINDINGS = [
        Binding("y", "answer(True)", "Yes"),
        Binding("n", "answer(False)", "No"),
        Binding("escape", "answer(False)", "No"),
    ]

    def __init__(self, question: str):
        super().__init__()
        self.question = question

    def compose(self) -> ComposeResult:
        yield Label(self.question)
        yield Label("[dim]y = yes, n / Esc = no[/dim]")

    def action_answer(self, yes: bool) -> None:
        self.dismiss(yes)


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
    
    def _matches(self, query: str) -> list[str]:
        """Commands for the typed text: exact, then name prefix, then anywhere."""
        query = query.strip().lower()
        hits = [cmd for cmd, desc in self.commands if query in cmd or query in desc.lower()]
        return sorted(hits, key=lambda cmd: (cmd != query, not cmd.startswith(query)))

    def _items(self, query: str = "") -> list[ListItem]:
        # the command lives in ListItem.name — str(Label) is just "Label()"
        descs = dict(self.commands)
        return [ListItem(Label(f"{cmd:<15} {descs[cmd]}"), name=cmd) for cmd in self._matches(query)]

    def compose(self) -> ComposeResult:
        yield Input(placeholder="Type command...", id="cmd_input")
        yield ListView(*self._items(), id="cmd_list")

    async def on_input_changed(self, event: Input.Changed):
        """Filter commands based on input."""
        list_view = self.query_one("#cmd_list", ListView)
        items = self._items(event.value)
        await list_view.clear()
        await list_view.extend(items)
        list_view.index = 0 if items else None  # clear() drops the highlight; Enter needs one

    def on_input_submitted(self, event: Input.Submitted):
        """Enter in the input runs the best match."""
        matches = self._matches(event.value)
        self.dismiss(matches[0] if matches else None)

    def on_list_view_selected(self, event: ListView.Selected):
        """Handle command selection."""
        self.dismiss(event.item.name)


class MeretApp(App):
    """Main TUI application for meret."""
    
    CSS = """
    Screen { align: center middle; }
    
    Horizontal {
        width: 100%;
        height: 1fr;  /* what's left after preview/path bar — 100% pushed them off-screen */
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
        Binding("U", "umount", "Unmount"),
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
        self.sort_by = _("name")  # name, size, date
        self.sort_reverse = False
    
    def on_mount(self) -> None:
        logger.info("TUI started: db=%s fs_root=%s", self.db_path, self.fs_root)

    def _handle_exception(self, error: Exception) -> None:
        # Textual's crash hook (private, but the only one): keep the traceback in the
        # log file — the screen version disappears with the terminal
        logger.critical("TUI crashed", exc_info=error)
        super()._handle_exception(error)

    def _run(self, cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
        """subprocess.run with the command and its exit code in the log."""
        logger.info("run: %s", cmd)
        result = subprocess.run(cmd, **kwargs)
        (logger.info if result.returncode == 0 else logger.warning)(
            "exit %s: %s", result.returncode, cmd
        )
        return result

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)

        with Horizontal():
            # Left panel: Use SortedDirectoryTree from start!
            self.real_tree = SortedDirectoryTree(self.fs_root, sort_by=self.sort_by, reverse=self.sort_reverse)
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
        """Handle real file selection: Enter/click opens it."""
        self.selected_real = event.path
        self.selected_virtual = None
        self.preview.update_preview(event.path, is_virtual=False)
        self._update_path_bar()
        self.action_open_file()

    def on_directory_tree_directory_selected(self, event: DirectoryTree.DirectorySelected):
        """Handle real directory selection."""
        if isinstance(event.node.data, ParentEntry):  # the '..' row
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
                except Exception:
                    logger.exception("select/open %s failed", self.selected_virtual)
    
    def action_command_palette(self):
        """Open command palette."""
        def on_command(cmd: str | None):
            if cmd:
                self.run_command(cmd)
        
        self.push_screen(CommandPalette(), on_command)
    
    def run_command(self, cmd: str):
        """Execute a command picked in the palette."""
        actions = {
            "scan": self.action_scan,
            "search": self.action_search,
            "tag": self.action_tag,
            "init": self.action_init_db,
            "config": self.action_config,
            "stats": self.action_stats,
            "tree": self.action_tree,
            "prune": self.action_prune,
            "mount": self.action_mount,
            "umount": self.action_umount,
        }
        logger.info("palette: %s", cmd)
        action = actions.get(cmd)
        if action is None:
            self.notify(f"'{cmd}' isn't available in the TUI yet", severity="warning")
            return
        action()
    
    def action_scan(self):
        """Scan current directory."""
        if self.selected_real and self.selected_real.is_dir():
            self.notify(f"Scanning {self.selected_real}...")
            self._run(["meret", "scan", str(self.selected_real), "--db", str(self.db_path)])
            self.virt_tree._load_tree()
            self.notify("Scan complete!")
    
    def action_open_file(self):
        """Open selected file with default app."""
        if self.selected_real and self.selected_real.is_file():
            if os.name == 'nt':
                os.startfile(self.selected_real)
            else:
                self._run(["xdg-open", str(self.selected_real)])
            logger.info("opened %s", self.selected_real)
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
                    self._run(["xdg-open", str(real_path)])

                logger.info("opened %s -> %s", self.selected_virtual, real_path)
                self.notify(f"Opened {self.selected_virtual} → {real_path.name}")

            except Exception as e:
                logger.exception("opening %s failed", self.selected_virtual)
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
                    logger.info("tagged %s with %r", self.selected_real, tag)
                    self.notify(f"Tagged with '{tag}'")
                    self.virt_tree._load_tree()

                except Exception as e:
                    logger.exception("tagging %s with %r failed", self.selected_real, tag)
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
    
    def action_tree(self):
        """The tag tree is the virtual panel: reload it and move focus there."""
        self.virt_tree._load_tree()
        self.virt_tree.focus()

    def action_prune(self):
        """Remove tags that have no files and no subtags."""
        try:
            removed = get_repo(self.db_path).prune_orphan_tags()
        except DatabaseNotFound:
            self.notify("No database!", severity="error")
            return
        logger.info("pruned %d unused tag(s)", removed)
        self.virt_tree._load_tree()
        self.notify(f"Removed {removed} unused tag(s)")

    def action_delete(self):
        """Delete the tag under the cursor in the virtual panel, after confirmation.

        Files are untouched; the tag's subtags move up to its parent.
        """
        vpath = None
        if self.focused is self.virt_tree and self.virt_tree.cursor_node is not None:
            vpath = self.virt_tree.cursor_node.data
        vpath = vpath or self.selected_virtual
        if not vpath:
            self.notify("Select a tag folder in the virtual panel first", severity="warning")
            return

        try:
            repo = get_repo(self.db_path)
            node = VirtualTree(repo).stat(vpath)
        except (DatabaseNotFound, NotFound) as e:
            self.notify(f"Error: {e}", severity="error")
            return
        if not node.is_dir or node.tag_id is None:
            self.notify("Select a tag folder (not a file or the root)", severity="warning")
            return

        # the vpath shows sanitized names — delete by the tag's real path
        by_id = {t.id: t for t in repo.get_all_tags()}
        path = tagpaths.path_of(by_id[node.tag_id], by_id)

        def on_answer(yes: bool | None):
            if not yes:
                return
            try:
                repo.delete_tag(path)
            except Exception as e:  # e.g. a subtag clashes with a same-named sibling
                logger.exception("deleting tag %r failed", path)
                self.notify(f"Error: {e}", severity="error")
                return
            logger.info("deleted tag %r", path)
            self.selected_virtual = None
            self.virt_tree._load_tree()
            self.notify(f"Deleted tag {path}")

        self.push_screen(
            ConfirmDialog(f"Delete tag '{path}'? Files stay; its subtags move up."), on_answer
        )

    def action_config(self):
        """Open config editor."""
        self._run(["meret", "config", "edit"])
    
    def action_init_db(self):
        """Initialize database."""
        self._run(["meret", "init", str(self.db_path)])
        self.virt_tree._load_tree()
        self.notify(_("Database initialized!"))
    
    def action_refresh(self):
        """Refresh both panels."""
        self.real_tree.reload()
        self.virt_tree._load_tree()
        self.notify(_("Refreshed"))
    
    def action_go_home(self):
        """Navigate to home directory."""
        self.fs_root = Path.home()
        self._refresh_fs_tree()
    
    def action_go_up(self):
        """Go to parent directory."""
        here = self.fs_root.expanduser().resolve()
        if here.parent == here:
            # root of a drive (C:\) or of the filesystem: on Windows pick another drive
            if platform.system() == "Windows":
                self.action_select_drive()
            return

        self.fs_root = here.parent
        self._refresh_fs_tree()

    def action_sort_files(self):
        """Toggle sort mode."""
        modes = [_("name"), _("size"), _("date")]
        idx = modes.index(self.sort_by)
        self.sort_by = modes[(idx + 1) % len(modes)]
        self.notify(_("Sort by: {mode}").format(mode=self.sort_by))
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
        """Point the real-FS panel at fs_root with the current sort mode.

        The same widget is reused: setting `path` makes DirectoryTree reload
        (always_update=True, so a sort change on the same folder reloads too).
        """
        if not hasattr(self, 'real_tree'):
            return

        logger.info("real panel -> %s (sort=%s)", self.fs_root, self.sort_by)
        self.real_tree.sort_by = self.sort_by
        self.real_tree.reverse = self.sort_reverse
        # not "[name]": border titles are markup, so square brackets would vanish
        self.real_tree.border_title = f"Real Filesystem ({self.fs_root}) · {self.sort_by}"
        self.real_tree.path = self.fs_root
        self.real_tree.focus()
    
    def action_help(self):
        """Show help."""
        help_text = """
        [b]Meret TUI Help[/b]

        [green]Navigation:[/green]
        ↑/↓            Navigate
        Enter          Open file / expand folder
        .. or u/Bksp   Go to parent directory
        h or ~         Go to home directory
        D              Select drive (Windows)
        S              Cycle sort: name / size / date
        q              Quit

        [green]Commands:[/green]
        :              Command palette
        /              Search
        s              Scan selected directory
        t              Tag selected file
        d              Delete tag (virtual panel)
        r              Refresh
        m / U          Mount / unmount
        c              Config
        i              Init DB
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
                            logger.exception("mount at %s failed", path)
                            self.call_from_thread(
                                lambda: self.notify(f"Mount error: {e}", severity="error")
                            )
                    
                    threading.Thread(target=mount_thread, daemon=True).start()
                    self.notify(f"Mounting at {path}... (Ctrl+C in console to exit)")
                    
                except ImportError as e:
                    logger.warning("mount: no driver: %s", e)
                    self.notify(f"Missing driver: {e}", severity="error")
                    self.notify("Install: pip install fusepy (Linux/Mac) or winfspy (Windows)")
                except Exception as e:
                    logger.exception("mount at %s failed", path)
                    self.notify(f"Error: {e}", severity="error")
        
        self.push_screen(InputDialog("Enter mount point (e.g., M: or /mnt/meret):"), on_mountpoint)


    def action_umount(self):
        """Unmount filesystem."""
        def on_unmount(path: str | None):
            if path:
                try:
                    self._run(["meret", "umount", path], check=True)
                    self.notify(f"Unmounted {path}")
                except Exception as e:
                    logger.exception("umount %s failed", path)
                    self.notify(f"Error: {e}", severity="error")
        
        self.push_screen(InputDialog("Enter mount point to unmount:"), on_unmount)

class ParentEntry(DirEntry):
    """Data of the '..' row. Selecting it moves the panel one level up."""


class SortedDirectoryTree(DirectoryTree):
    """DirectoryTree with sorting and a '..' row on top of the root listing.

    Everything goes through DirectoryTree's own hooks: loading stays in its
    worker, nodes keep real DirEntry data, so expanding/reloading keep working.
    Changing the root (going up, home, another drive) = setting `path`.
    """

    def __init__(self, path: Path, sort_by: str = "name", reverse: bool = False, **kwargs):
        self.sort_by = sort_by
        self.reverse = reverse
        super().__init__(path, **kwargs)

    def _sort_key(self, path: Path):
        """name / size / date. Folders have no meaningful size — by name."""
        try:
            if self.sort_by == "size" and not self._safe_is_dir(path):
                return (0, path.stat().st_size, "")
            if self.sort_by == "date":
                return (0, path.stat().st_mtime, "")
        except OSError:
            pass
        return (1, 0, path.name.lower())

    def _populate_node(self, node, content) -> None:
        paths = list(content)
        dirs = [p for p in paths if self._safe_is_dir(p)]
        files = [p for p in paths if not self._safe_is_dir(p)]
        # folders always on top; the sort mode orders each group
        ordered = sorted(dirs, key=self._sort_key, reverse=self.reverse) + sorted(
            files, key=self._sort_key, reverse=self.reverse
        )
        super()._populate_node(node, ordered)

        if node is self.root:
            here = Path(self.path).expanduser().resolve()
            if here.parent != here:  # a drive / filesystem root has nowhere to go
                node.add("..", data=ParentEntry(here.parent), before=0, allow_expand=False)

    def render_label(self, node, base_style, style) -> Text:
        if isinstance(node.data, ParentEntry):
            label = node.label.copy()
            label.stylize(style)
            return Text.assemble(("⬆ ", base_style), label)
        return super().render_label(node, base_style, style)


def main():
    """Entry point for TUI."""
    import sys
    setup_logging(console=False)
    log_uncaught_exceptions()
    db_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(".")
    app = MeretApp(db_path)
    app.run()


if __name__ == "__main__":
    main()