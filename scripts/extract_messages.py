#!/usr/bin/env python3
"""Extract translatable strings from source code."""

import re
from pathlib import Path

def extract_strings():
    """Find all _('...') and _('...', ...) calls in meret/."""
    pattern = re.compile(r"""_��(['\"])(.+?)\1��(?:*,*context*=*(['\"])(.+?)\3)?""", re.VERBOSE)
    
    strings = set()
    
    for py_file in Path("meret").rglob("*.py"):
        content = py_file.read_text(encoding="utf-8")
        for match in pattern.finditer(content):
            msgid = match.group(2)
            context = match.group(4)
            if context:
                strings.add((f"pgettext: {context}", msgid))
            else:
                strings.add(("", msgid))
    
    # Sort and print
    for ctx, msg in sorted(strings):
        if ctx:
            print(f"# {ctx}")
        print(f'msgid "{msg}"')
        print('msgstr ""')
        print()

if __name__ == "__main__":
    extract_strings()