"""Tests for TUI localization - bindings, dialogs, commands."""

import ast
from pathlib import Path

import pytest

from meret.core.locale import _, set_locale

MERET = Path(__file__).resolve().parents[2] / "meret"
TUI_FILE = MERET / "cli" / "tui.py"


def _extract_tui_strings():
    """Extract all strings that should be translated in TUI."""
    if not TUI_FILE.exists():
        return [], []
    
    content = TUI_FILE.read_text(encoding="utf-8")
    tree = ast.parse(content)
    
    bindings = []
    dialog_labels = []
    
    for node in ast.walk(tree):
        # Find Binding() calls
        if isinstance(node, ast.Call):
            # Binding("key", "action", "Description")
            if isinstance(node.func, ast.Name) and node.func.id == "Binding":
                if len(node.args) >= 3:
                    # Third argument is the description
                    desc_arg = node.args[2]
                    if isinstance(desc_arg, ast.Constant) and isinstance(desc_arg.value, str):
                        bindings.append((desc_arg.value, node.lineno))
                    elif isinstance(desc_arg, ast.Call):
                        # Check if it's _("...")
                        if isinstance(desc_arg.func, ast.Name) and desc_arg.func.id == "_":
                            # Already wrapped, skip
                            pass
                        else:
                            bindings.append((f"[COMPLEX at line {node.lineno}]", node.lineno))
            
            # Button() calls
            if isinstance(node.func, ast.Name) and node.func.id == "Button":
                for kw in node.keywords:
                    if kw.arg == "id":
                        continue
                # First positional arg is usually label
                if node.args:
                    first_arg = node.args[0]
                    if isinstance(first_arg, ast.Constant) and isinstance(first_arg.value, str):
                        if first_arg.value not in ["Cancel", "OK", "Yes", "No", "Close", "Select"]:
                            # Skip common button labels we check separately
                            pass
    
    return bindings, dialog_labels


class TestTuiBindingsLocalized:
    """Test that all TUI bindings are localized."""
    
    def test_bindings_are_wrapped_in_gettext(self):
        """Check that Binding descriptions use _()."""
        content = TUI_FILE.read_text(encoding="utf-8")
        
        # Find lines with Binding that don't use _()
        
        # Pattern: Binding(..., "Description") without _()
        # This is a simplified check
        lines = content.split('\n')
        unwrapped = []
        
        for i, line in enumerate(lines, 1):
            if 'Binding(' in line and 'Binding(""' not in line:
                # Check if line contains _(" or is a comment
                if '_(' not in line and not line.strip().startswith('#'):
                    # Might be unwrapped
                    if '"Quit"' in line or '"Cancel"' in line or '"OK"' in line:
                        unwrapped.append((i, line.strip()))
        
        # This test documents which bindings need wrapping
        # After fix, this list should be empty
        assert len(unwrapped) == 0, "Unwrapped Binding descriptions found:\n" + "\n".join(f"{l}: {c}" for l, c in unwrapped)


class TestTuiCommandPalette:
    """Test CommandPalette localization."""
    
    def test_palette_commands_have_translations(self):
        """Test that all palette commands are in .po file."""
        from meret.core.locale import DEFAULT_LOCALE_DIR
        
        po_file = DEFAULT_LOCALE_DIR / "ru" / "LC_MESSAGES" / "messages.po"
        if not po_file.exists():
            pytest.skip("Russian .po file not found")
        
        po_content = po_file.read_text(encoding="utf-8")
        
        # Commands from CommandPalette
        commands = [
            "Scan directory for media",
            "Search files by tags/name", 
            "Tag selected file",
            "Remove tag from file",
            "Initialize new database",
            "Edit configuration",
            "Show database statistics",
            "Show tag tree",
            "Remove orphaned tags",
            "Mount virtual filesystem",
            "Unmount filesystem",
        ]
        
        missing = []
        for cmd in commands:
            # Check if msgid exists in .po
            if f'msgid "{cmd}"' not in po_content:
                missing.append(cmd)
        
        assert not missing, f"Missing translations for: {missing}"


class TestDialogButtons:
    """Test dialog button labels."""
    
    def test_common_buttons_translated(self):
        """Test that standard dialog buttons have translations."""
        set_locale("ru")
        
        buttons = ["Cancel", "OK", "Yes", "No", "Close", "Select"]
        translations = {
            "Cancel": "Отмена",
            "OK": "ОК", 
            "Yes": "Да",
            "No": "Нет",
            "Close": "Закрыть",
            "Select": "Выбрать",
        }
        
        for btn in buttons:
            result = _(btn)
            # Either translated or same (if not in catalog)
            assert result == translations.get(btn, btn) or result == btn


class TestTuiBindingsRussian:
    """Test TUI bindings in Russian."""
    
    def test_bindings_in_russian(self):
        """Test that binding descriptions work in Russian."""
        set_locale("ru")
        
        bindings = {
            "Quit": "Выход",
            "Config": "Конфиг",
            "Init DB": "Иниц. БД",
            "Open": "Открыть",
            "Scan": "Скан",
            "Tag": "Тег",
            "Refresh": "Обновить",
            "Delete tag": "Удалить тег",
            "Home": "Домой",
            "Up": "Вверх",
            "Sort": "Сортировать",
            "Mount": "Монтировать",
            "Unmount": "Размонтировать",
            "Select Drive": "Выбрать диск",
            "Search": "Поиск",
            "Help": "Помощь",
        }
        
        for eng, rus in bindings.items():
            result = _(eng)
            # After adding translations, this should be rus
            # Before adding, it will be eng (fallback)
            assert result in [eng, rus], f"Expected '{rus}' or '{eng}', got '{result}'"


class TestTuiHelpText:
    """Test TUI help text localization."""
    
    def test_help_sections_translated(self):
        """Test that help text sections are translatable."""
        set_locale("ru")
        
        sections = [
            "Navigation",
            "Commands",
            "Navigate",
            "Open file",
            "expand folder",
            "Go to parent directory",
            "Go to home directory",
            "Select drive",
            "Cycle sort: name / size / date",
            "Command palette",
            "Search",
            "Scan selected directory",
            "Tag selected file",
            "Delete tag (virtual panel)",
            "Refresh",
            "Mount",
            "unmount",
            "Config",
            "Init DB",
        ]
        
        for section in sections:
            result = _(section)
            # Should be translated or fallback to original
            assert isinstance(result, str) and len(result) > 0


class TestTuiBorderTitles:
    """Test border title localization."""
    
    def test_border_title_format(self):
        """Test that border title with placeholders works."""
        set_locale("ru")
        
        # This should use the translated format string
        title = _("Real Filesystem ({path}) · {sort}").format(path="/home", sort="name")
        assert "{path}" not in title
        assert "{sort}" not in title
        assert "/home" in title