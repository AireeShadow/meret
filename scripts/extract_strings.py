#!/usr/bin/env python3
"""
Extract translatable strings from meret source code.
Finds all _('...') and _("...") calls and generates .pot template.
"""

import ast
import re
from pathlib import Path
from typing import Set, Tuple


class StringExtractor(ast.NodeVisitor):
    """AST visitor to extract _() calls."""
    
    def __init__(self):
        self.strings: Set[Tuple[str, str, int]] = set()  # (msgid, context, line)
        self.plurals: Set[Tuple[str, str, int]] = set()   # (singular, plural, line)
        self.current_file = ""
    
    def visit_Call(self, node):
        """Visit function calls."""
        if isinstance(node.func, ast.Name):
            func_name = node.func.id
            
            if func_name in ('_', 'gettext'):
                # Simple translation: _('text') or _('text', context='...')
                if node.args:
                    first_arg = node.args[0]
                    if isinstance(first_arg, ast.Constant) and isinstance(first_arg.value, str):
                        msgid = first_arg.value
                        context = self._get_context(node)
                        self.strings.add((msgid, context, node.lineno))
            
            elif func_name in ('ngettext', '_n'):
                # Plural translation: ngettext('one', 'many', n)
                if len(node.args) >= 2:
                    singular = node.args[0]
                    plural = node.args[1]
                    if (isinstance(singular, ast.Constant) and isinstance(singular.value, str) and
                        isinstance(plural, ast.Constant) and isinstance(plural.value, str)):
                        self.plurals.add((singular.value, plural.value, node.lineno))
        
        self.generic_visit(node)
    
    def _get_context(self, node) -> str:
        """Extract context=... from kwargs."""
        for kw in node.keywords:
            if kw.arg == 'context' and isinstance(kw.value, ast.Constant):
                return kw.value.value
        return ""


def extract_from_file(py_file: Path) -> Tuple[Set, Set]:
    """Extract strings from a single Python file."""
    try:
        tree = ast.parse(py_file.read_text(encoding="utf-8"), str(py_file))
    except SyntaxError as e:
        print(f"  Warning: Syntax error in {py_file}: {e}")
        return set(), set()
    
    extractor = StringExtractor()
    extractor.current_file = str(py_file)
    extractor.visit(tree)
    return extractor.strings, extractor.plurals


def extract_all_strings():
    """Extract strings from all Python files in meret/."""
    project_root = Path(__file__).parent.parent
    meret_dir = project_root / "meret"
    
    all_strings: Set[Tuple[str, str, int, str]] = set()  # (msgid, context, line, file)
    all_plurals: Set[Tuple[str, str, int, str]] = set()  # (sing, plur, line, file)
    
    print(f"Scanning {meret_dir}...")
    
    for py_file in meret_dir.rglob("*.py"):
        if "test" in py_file.name or "__pycache__" in str(py_file):
            continue
        
        strings, plurals = extract_from_file(py_file)
        
        for msgid, context, line in strings:
            all_strings.add((msgid, context, line, str(py_file.relative_to(project_root))))
        
        for sing, plur, line in plurals:
            all_plurals.add((sing, plur, line, str(py_file.relative_to(project_root))))
    
    # Generate .pot file
    pot_file = project_root / "meret" / "locales" / "messages.pot"
    pot_file.parent.mkdir(parents=True, exist_ok=True)
    
    lines = [
        '# Translations template for meret',
        '# Copyright (C) 2024',
        '# This file is distributed under the same license as the meret package.',
        '#',
        '#, fuzzy',
        'msgid ""',
        'msgstr ""',
        '"Project-Id-Version: meret 0.5.10\\n"',
        '"Report-Msgid-Bugs-To: \\n"',
        '"POT-Creation-Date: 2024-01-01 00:00+0000\\n"',
        '"PO-Revision-Date: YEAR-MO-DA HO:MI+ZONE\\n"',
        '"Last-Translator: FULL NAME <EMAIL@ADDRESS>\\n"',
        '"Language-Team: LANGUAGE <LL@li.org>\\n"',
        '"MIME-Version: 1.0\\n"',
        '"Content-Type: text/plain; charset=UTF-8\\n"',
        '"Content-Transfer-Encoding: 8bit\\n"',
        '"Plural-Forms: nplurals=INTEGER; plural=EXPRESSION;\\n"',
        '',
    ]
    
    # Add regular strings
    for msgid, context, line, file in sorted(all_strings):
        lines.append(f'#: {file}:{line}')
        if context:
            lines.append(f'msgctxt "{context}"')
        lines.append(f'msgid "{_escape(msgid)}"')
        lines.append('msgstr ""')
        lines.append('')
    
    # Add plural strings
    for sing, plur, line, file in sorted(all_plurals):
        lines.append(f'#: {file}:{line}')
        lines.append(f'msgid "{_escape(sing)}"')
        lines.append(f'msgid_plural "{_escape(plur)}"')
        lines.append('msgstr[0] ""')
        lines.append('msgstr[1] ""')
        lines.append('')
    
    pot_file.write_text('\n'.join(lines), encoding="utf-8")
    
    print(f"\n✓ Extracted {len(all_strings)} strings and {len(all_plurals)} plurals")
    print(f"✓ Written to {pot_file}")
    
    # Show sample
    print(f"\nSample entries:")
    for msgid, _, _, _ in list(all_strings)[:5]:
        print(f'  - "{msgid[:50]}"')


def _escape(s: str) -> str:
    """Escape quotes and newlines for .pot file."""
    return s.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n').replace('\r', '')


if __name__ == "__main__":
    extract_all_strings()