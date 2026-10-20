#!/usr/bin/env python3
"""Fix remaining untranslated strings in tui.py."""

from pathlib import Path

def fix_tui():
    tui_file = Path(__file__).parent.parent / "meret" / "cli" / "tui.py"
    content = tui_file.read_text(encoding="utf-8")
    
    # Проверяем, что есть импорт _
    if "from meret.core.locale import _" not in content:
        print("ERROR: Missing import of _ from meret.core.locale")
        return
    
    replacements = {
        # DriveSelector
        'yield Label("Select Drive:")': 'yield Label(_("Select Drive:"))',
        
        # FilePreview
        'super().__init__("Select a file to preview': 'super().__init__(_("Select a file to preview',
        'self.update("[dim]No selection[/dim]")': 'self.update(_("[dim]No selection[/dim]"))',
        'self.update(f"[cyan]Virtual:[/cyan] {path}\\n[dim]Use \'o\' to open real file[/dim]")':
            'self.update(_("[cyan]Virtual:[/cyan] {path}\\n[dim]Use \'o\' to open real file[/dim]").format(path=path))',
        'self.update(f"[green]{path.name}[/green]\\nSize: {size}\\nModified: {stat.st_mtime}")':
            'self.update(_("[green]{name}[/green]\\nSize: {size}\\nModified: {mtime}").format(name=path.name, size=size, mtime=stat.st_mtime))',
        'self.update("[red]Cannot access file[/red]")': 'self.update(_("[red]Cannot access file[/red]"))',
        
        # VirtualFileTree
        'self.root.add_leaf("[dim]No database - run \'meret init\'[/dim]")':
            'self.root.add_leaf(_("[dim]No database - run \'meret init\'[/dim]"))',
        'self.root.add_leaf(f"[red]Error: {escape(str(e))}[/red] [dim](meret logs)[/dim]")':
            'self.root.add_leaf(_("[red]Error: {error}[/red] [dim](meret logs)[/dim]").format(error=escape(str(e))))',
        'node.add_leaf("[dim](empty)[/dim]")': 'node.add_leaf(_("[dim](empty)[/dim]"))',
        'node.add_leaf(f"[red]Error: empty vpath for {entry.name}[/red]")':
            'node.add_leaf(_("[red]Error: empty vpath for {name}[/red]").format(name=entry.name))',
        
        # TagDialog
        'yield Label(f"Tag file: {self.file_path.name}")': 'yield Label(_("Tag file: {name}").format(name=self.file_path.name))',
        'yield Input(placeholder="Enter tag (e.g., vacation/beach)", id="tag_input")':
            'yield Input(placeholder=_("Enter tag (e.g., vacation/beach)"), id="tag_input")',
        'yield Label("[dim]Press Enter to confirm, Esc to cancel[/dim]")':
            'yield Label(_("[dim]Press Enter to confirm, Esc to cancel[/dim]"))',
        
        # SearchScreen
        'yield Label(f"Search: {self.query}")': 'yield Label(_("Search: {query}").format(query=self.query))',
        'yield Label("[dim]↑↓ to navigate, Enter to select, Esc to close[/dim]")':
            'yield Label(_("[dim]↑↓ to navigate, Enter to select, Esc to close[/dim]"))',
        
        # ConfirmDialog
        'yield Label("[dim]y = yes, n / Esc = no[/dim]")':
            'yield Label(_("[dim]y = yes, n / Esc = no[/dim]"))',
        
        # CommandPalette
        'yield Input(placeholder="Type command...", id="cmd_input")':
            'yield Input(placeholder=_("Type command..."), id="cmd_input")',
        
        # CommandPalette commands (кортежи)
        '("scan", "Scan directory for media")': '("scan", _("Scan directory for media"))',
        '("search", "Search files by tags/name")': '("search", _("Search files by tags/name"))',
        '("tag", "Tag selected file")': '("tag", _("Tag selected file"))',
        '("untag", "Remove tag from file")': '("untag", _("Remove tag from file"))',
        '("init", "Initialize new database")': '("init", _("Initialize new database"))',
        '("config", "Edit configuration")': '("config", _("Edit configuration"))',
        '("stats", "Show database statistics")': '("stats", _("Show database statistics"))',
        '("tree", "Show tag tree")': '("tree", _("Show tag tree"))',
        '("prune", "Remove orphaned tags")': '("prune", _("Remove orphaned tags"))',
        '("mount", "Mount virtual filesystem")': '("mount", _("Mount virtual filesystem"))',
        '("umount", "Unmount filesystem")': '("umount", _("Unmount filesystem"))',
        
        # MeretApp notifications
        'self.notify(f"Scanning {self.selected_real}...")':
            'self.notify(_("Scanning {path}...").format(path=self.selected_real))',
        'self.notify("Scan complete!")': 'self.notify(_("Scan complete!"))',
        'self.notify(f"Opened {self.selected_real.name}")':
            'self.notify(_("Opened {name}").format(name=self.selected_real.name))',
        'self.notify(f"Opened {self.selected_virtual} → {real_path.name}")':
            'self.notify(_("Opened {virtual} → {real}").format(virtual=self.selected_virtual, real=real_path.name))',
        'self.notify("Select a real file first!", severity="error")':
            'self.notify(_("Select a real file first!"), severity="error")',
        'self.notify(f"Tagged with \'{tag}\'")': 'self.notify(_("Tagged with \'{tag}\'").format(tag=tag))',
        'self.notify(f"Selected: {result.filename}")':
            'self.notify(_("Selected: {filename}").format(filename=result.filename))',
        'self.notify("No database!", severity="error")':
            'self.notify(_("No database!"), severity="error")',
        'self.notify(f"Files: {stats[\'total_files\']}, Tags: {stats[\'total_tags\']}")':
            'self.notify(_("Files: {files}, Tags: {tags}").format(files=stats["total_files"], tags=stats["total_tags"]))',
        'self.notify(f"Removed {removed} unused tag(s)")':
            'self.notify(_("Removed {n} unused tag(s)").format(n=removed))',
        'self.notify("Select a tag folder in the virtual panel first", severity="warning")':
            'self.notify(_("Select a tag folder in the virtual panel first"), severity="warning")',
        'self.notify("Select a tag folder (not a file or the root)", severity="warning")':
            'self.notify(_("Select a tag folder (not a file or the root)"), severity="warning")',
        'self.notify(f"Deleted tag {path}")': 'self.notify(_("Deleted tag {path}").format(path=path))',
        'self.notify("Drive selection only available on Windows", severity="warning")':
            'self.notify(_("Drive selection only available on Windows"), severity="warning")',
        'self.notify(f"Switched to {drive}")': 'self.notify(_("Switched to {drive}").format(drive=drive))',
        'self.notify(f"Mounting at {path}...")': 'self.notify(_("Mounting at {path}...").format(path=path))',
        'self.notify(f"Mounting at {path}... (Ctrl+C in console to exit)")':
            'self.notify(_("Mounting at {path}... (Ctrl+C in console to exit)").format(path=path))',
        'self.notify(f"Missing driver: {e}", severity="error")':
            'self.notify(_("Missing driver: {error}").format(error=e), severity="error")',
        'self.notify("Install: pip install fusepy (Linux/Mac) or winfspy (Windows)")':
            'self.notify(_("Install: pip install fusepy (Linux/Mac) or winfspy (Windows)"))',
        'self.notify(f"Unmounted {path}")': 'self.notify(_("Unmounted {path}").format(path=path))',
        'self.notify(f"\'{cmd}\' isn\'t available in the TUI yet", severity="warning")':
            'self.notify(_("\'{cmd}\' isn\'t available in the TUI yet").format(cmd=cmd), severity="warning")',
        
        # Border titles
        'self.border_title = "Virtual FS (Tags)"': 'self.border_title = _("Virtual FS (Tags)")',
        'self.real_tree.border_title=f"Real Filesystem ({self.fs_root}) · {self.sort_by}"':
            'self.real_tree.border_title=_("Real Filesystem ({path}) · {sort}").format(path=self.fs_root, sort=self.sort_by)',
        
        # Help text
        'help_text="""': 'help_text=_("""',
    }
    
    # Применяем замены
    count = 0
    for old, new in replacements.items():
        if old in content:
            content = content.replace(old, new)
            count += 1
    
    # Закрывающие кавычки для help_text (только если мы открыли с _(""")
    if 'help_text=_("""' in content and '"""' in content:
        # Находим позицию после help_text и заменяем первые """ после него
        idx = content.find('help_text=_("""')
        if idx > 0:
            # Ищем закрывающие """ после открывающих
            start = idx + len('help_text=_("""')
            end = content.find('"""', start)
            if end > start:
                # Заменяем """ на """)  — но осторожно, не трогаем другие
                content = content[:end] + '""")' + content[end+3:]
    
    tui_file.write_text(content, encoding="utf-8")
    print(f"✓ Fixed {count} strings in {tui_file}")


if __name__ == "__main__":
    fix_tui()