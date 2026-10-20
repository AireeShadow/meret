#!/usr/bin/env python3
"""Auto-translate tui.py strings."""

import re
from pathlib import Path

def translate_tui():
    tui_file = Path(__file__).parent.parent / "meret" / "cli" / "tui.py"
    content = tui_file.read_text(encoding="utf-8")
    
    # Проверяем, что уже есть импорт _
    if "from meret.core.locale import _" not in content:
        # Добавляем импорт после существующих импортов
        content = content.replace(
            "from meret.vfs import IsADirectory, NotFound, VirtualTree",
            "from meret.core.locale import _\nfrom meret.vfs import IsADirectory, NotFound, VirtualTree"
        )
    
    # Словарь замен: старая строка → новая
    replacements = {
        # FilePreview
        'yield Label("Select a file to preview")': 'yield Label(_("Select a file to preview"))',
        'self.border_title = "Preview"': 'self.border_title = _("Preview")',
        'self.update("No selection")': 'self.update(_("No selection"))',
        'self.update(f"Virtual: {node.vpath}\\nUse \'o\' to open real file")': 
            'self.update(_("Virtual: {path}\\nUse \'o\' to open real file").format(path=node.vpath))',
        'self.update(f"File: {real}\\nSize: {size} bytes")':
            'self.update(_("File: {path}\\nSize: {size} bytes").format(path=real, size=size))',
        'self.update("Cannot access file")': 'self.update(_("Cannot access file"))',
        'self.update(f"Error: {e}")': 'self.update(_("Error: {error}").format(error=e))',
        
        # TagDialog
        'yield Label(f"Tag file: {self.filename}")': 'yield Label(_("Tag file: {filename}").format(filename=self.filename))',
        'yield Input(placeholder="Enter tag (e.g., vacation/beach)")': 
            'yield Input(placeholder=_("Enter tag (e.g., vacation/beach)"))',
        'yield Label("Press Enter to confirm, Esc to cancel")': 
            'yield Label(_("Press Enter to confirm, Esc to cancel"))',
        
        # SearchDialog
        'yield Label(f"Search: {self.query or \'(all files)\'}")':
            'yield Label(_("Search: {query}").format(query=self.query or _("(all files)")))',
        'yield Input(placeholder="Enter search query")': 'yield Input(placeholder=_("Enter search query"))',
        'yield Label("↑↓ to navigate, Enter to select, Esc to close")':
            'yield Label(_("↑↓ to navigate, Enter to select, Esc to close"))',
        
        # ConfirmDialog
        'yield Label("y = yes, n / Esc = no")': 'yield Label(_("y = yes, n / Esc = no"))',
        
        # CommandPalette
        '("Scan directory for media", "scan")': '(_("Scan directory for media"), "scan")',
        '("Search files by tags/name", "search")': '(_("Search files by tags/name"), "search")',
        '("Tag selected file", "tag")': '(_("Tag selected file"), "tag")',
        '("Remove tag from file", "untag")': '(_("Remove tag from file"), "untag")',
        '("Initialize new database", "init")': '(_("Initialize new database"), "init")',
        '("Edit configuration", "config")': '(_("Edit configuration"), "config")',
        '("Show database statistics", "stats")': '(_("Show database statistics"), "stats")',
        '("Show tag tree", "tree")': '(_("Show tag tree"), "tree")',
        '("Remove orphaned tags", "prune")': '(_("Remove orphaned tags"), "prune")',
        '("Mount virtual filesystem", "mount")': '(_("Mount virtual filesystem"), "mount")',
        '("Unmount filesystem", "umount")': '(_("Unmount filesystem"), "umount")',
        'yield Input(placeholder="Type command...")': 'yield Input(placeholder=_("Type command..."))',
        
        # MeretApp bindings
        'Binding("q", "Quit", "Quit")': 'Binding("q", _("Quit"), _("Quit"))',
        'Binding(":", "Command", "Command")': 'Binding(":", _("Command"), _("Command"))',
        'Binding("i", "Init DB", "Init DB")': 'Binding("i", _("Init DB"), _("Init DB"))',
        'Binding("o", "Open", "Open")': 'Binding("o", _("Open"), _("Open"))',
        'Binding("s", "Scan", "Scan")': 'Binding("s", _("Scan"), _("Scan"))',
        'Binding("t", "Tag", "Tag")': 'Binding("t", _("Tag"), _("Tag"))',
        'Binding("r", "Refresh", "Refresh")': 'Binding("r", _("Refresh"), _("Refresh"))',
        'Binding("c", "Config", "Config")': 'Binding("c", _("Config"), _("Config"))',
        'Binding("d", "Delete tag", "Delete tag")': 'Binding("d", _("Delete tag"), _("Delete tag"))',
        'Binding("h", "Home", "Home")': 'Binding("h", _("Home"), _("Home"))',
        'Binding("u", "Up", "Up")': 'Binding("u", _("Up"), _("Up"))',
        'Binding("S", "Sort", "Sort")': 'Binding("S", _("Sort"), _("Sort"))',
        'Binding("m", "Mount", "Mount")': 'Binding("m", _("Mount"), _("Mount"))',
        'Binding("U", "Unmount", "Unmount")': 'Binding("U", _("Unmount"), _("Unmount"))',
        'Binding("D", "Select Drive", "Select Drive")': 'Binding("D", _("Select Drive"), _("Select Drive"))',
        'Binding("/", "Search", "Search")': 'Binding("/", _("Search"), _("Search"))',
        'Binding("?", "Help", "Help")': 'Binding("?", _("Help"), _("Help"))',
        
        # MeretApp methods
        'self.sort_by = "name"': 'self.sort_by = _("name")',
        'Static("Loading...", id="left")': 'Static(_("Loading..."), id="left")',
        'Static("Loading...", id="right")': 'Static(_("Loading..."), id="right")',
        'self.title = "Meret TUI"': 'self.title = _("Meret TUI")',
        'left.update("No database - run \'meret init\'")':
            'left.update(_("No database - run \'meret init\'"))',
        'left.update("Virtual FS (Tags)")': 'left.update(_("Virtual FS (Tags)"))',
        'right.update(f"Real Filesystem ({Path.cwd()})")':
            'right.update(_("Real Filesystem ({path})").format(path=Path.cwd()))',
        
        # Notifications
        'self.notify("Command: {cmd}".format(cmd=result))':
            'self.notify(_("Command: {cmd}").format(cmd=result))',
        'self.notify("Database initialized!")': 'self.notify(_("Database initialized!"))',
        'self.notify("Opened file")': 'self.notify(_("Opened file"))',
        'self.notify("Scanning...")': 'self.notify(_("Scanning..."))',
        'self.notify("Tagged with \'{tag}\'".format(tag=result))':
            'self.notify(_("Tagged with \'{tag}\'").format(tag=result))',
        'TagDialog("selected file")': 'TagDialog(_("selected file"))',
        'self.notify("Refreshed")': 'self.notify(_("Refreshed"))',
        'self.notify("Configuration")': 'self.notify(_("Configuration"))',
        'self.notify("Delete tag")': 'self.notify(_("Delete tag"))',
        'self.notify("Home directory")': 'self.notify(_("Home directory"))',
        'self.notify("Go up")': 'self.notify(_("Go up"))',
        'modes = ["name", "size", "date"]': 'modes = [_("name"), _("size"), _("date")]',
        'self.notify(f"Sort by: {self.sort_by}")':
            'self.notify(_("Sort by: {mode}").format(mode=self.sort_by))',
        'self.notify("Mounting...")': 'self.notify(_("Mounting..."))',
        'self.notify("Unmounting...")': 'self.notify(_("Unmounting..."))',
        'self.notify("Drive selection only available on Windows")':
            'self.notify(_("Drive selection only available on Windows"))',
        'self.notify("Select drive")': 'self.notify(_("Select drive"))',
        'self.notify(f"Search: {result}")': 'self.notify(_("Search: {query}").format(query=result))',
        
        # Help text
        'title="Help"': 'title=_("Help")',
    }
    
    # Применяем замены
    for old, new in replacements.items():
        content = content.replace(old, new)
    
    # Многострочный help text
    old_help = '''        help_text = (
            "Navigation:\\n"
            "  ↑/↓ Navigate\\n"
            "  Enter Open file / expand folder\\n"
            "  .. or u/Backspace Go to parent\\n"
            "  h or ~ Go home\\n"
            "  D Select drive (Windows)\\n"
            "  S Cycle sort: name / size / date\\n"
            "Commands:\\n"
            "  q Quit\\n"
            "  : Command palette\\n"
            "  / Search\\n"
            "  s Scan directory\\n"
            "  t Tag selected file\\n"
            "  d Delete tag\\n"
            "  r Refresh\\n"
            "  m/U Mount/Unmount\\n"
            "  c Config\\n"
            "  i Init DB"
        )'''
    
    new_help = '''        help_text = _(
            "Navigation:\\n"
            "  ↑/↓ Navigate\\n"
            "  Enter Open file / expand folder\\n"
            "  .. or u/Backspace Go to parent\\n"
            "  h or ~ Go home\\n"
            "  D Select drive (Windows)\\n"
            "  S Cycle sort: name / size / date\\n"
            "Commands:\\n"
            "  q Quit\\n"
            "  : Command palette\\n"
            "  / Search\\n"
            "  s Scan directory\\n"
            "  t Tag selected file\\n"
            "  d Delete tag\\n"
            "  r Refresh\\n"
            "  m/U Mount/Unmount\\n"
            "  c Config\\n"
            "  i Init DB"
        )'''
    
    content = content.replace(old_help, new_help)
    
    # RuntimeError в импорте
    content = content.replace(
        '"Textual is required for the TUI. Install: pip install textual"',
        '_("Textual is required for the TUI. Install: pip install textual")'
    )
    
    # Сохраняем
    tui_file.write_text(content, encoding="utf-8")
    print(f"✓ Translated {len(replacements)} strings in {tui_file}")


if __name__ == "__main__":
    translate_tui()