#!/usr/bin/env python3
"""
Auto-add _() calls to console.print() statements.
Finds patterns like console.print("[green]✓ Text...") and replaces with _().
"""

import re
from pathlib import Path

# Patterns to match
PATTERNS = [
    # console.print("[color]✓ Text: {var}") -> console.print(f"[color]✓ {_('Text')}: {var}")
    (
        r'console\.print\(\s*"(\[[\w]+\][✓✗×\s]*)([^"{]+?)([^"]*?)"\s*\)',
        r'console.print(f"\1{_(\'\\2\')}\\3")',
    ),
    # console.print(f"[color]✓ Text: {var}") with existing f-string
    (
        r'console\.print\(\s*f"(\[[\w]+\][✓✗×\s]*)([^"{]+?)([^"]*?)"\s*\)',
        r'console.print(f"\1{_(\'\\2\')}\\3")',
    ),
]

# Files to process
TARGET_FILES = [
    "meret/cli/app.py",
    "meret/cli/vfs_cmds.py",
    # Add more files here
]

def add_import(filepath: Path):
    """Add 'from meret.core.locale import _' if not present."""
    content = filepath.read_text(encoding="utf-8")
    
    if "from meret.core.locale import" in content:
        return  # Already imported
    
    # Find a good place to add import (after other meret imports)
    lines = content.split("\n")
    import_idx = 0
    for i, line in enumerate(lines):
        if line.startswith("from meret.") or line.startswith("import meret"):
            import_idx = i + 1
    
    lines.insert(import_idx, "from meret.core.locale import _")
    filepath.write_text("\n".join(lines), encoding="utf-8")
    print(f"  Added import to {filepath}")

def process_file(filepath: Path):
    """Process a single file."""
    print(f"Processing {filepath}...")
    
    content = filepath.read_text(encoding="utf-8")
    original = content
    
    # Simple replacements for common patterns
    replacements = [
        # "[green]✓ Database created" -> "[green]✓ " + _("Database created")
        (r'"\[green\]✓ ([^"]+?)"', r'"[green]✓ " + _("\1")'),
        (r'"\[red\]✗ ([^"]+?)"', r'"[red]✗ " + _("\1")'),
        (r'"\[yellow\]⚠ ([^"]+?)"', r'"[yellow]⚠ " + _("\1")'),
        (r'"\[cyan\] ([^"]+?)"', r'"[cyan\] " + _("\1")'),
    ]
    
    for pattern, repl in replacements:
        content = re.sub(pattern, repl, content)
    
    if content != original:
        filepath.write_text(content, encoding="utf-8")
        print(f"  ✓ Made replacements")
    else:
        print(f"  - No matches")

def main():
    project_root = Path(__file__).parent.parent
    
    for rel_path in TARGET_FILES:
        filepath = project_root / rel_path
        if filepath.exists():
            add_import(filepath)
            process_file(filepath)
        else:
            print(f"Warning: {filepath} not found")

if __name__ == "__main__":
    main()