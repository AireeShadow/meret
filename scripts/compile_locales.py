#!/usr/bin/env python3
"""Compile .po files to .mo files using polib."""

from pathlib import Path

import polib


def compile_locales():
    locales_dir = Path(__file__).parent.parent / "meret" / "locales"
    
    for po_file in locales_dir.rglob("*.po"):
        mo_file = po_file.parent / "messages.mo"
        print(f"Compiling {po_file} -> {mo_file}")
        
        po = polib.pofile(str(po_file))
        po.save_as_mofile(str(mo_file))
        
        print(f"  ✓ Done ({len(po.translated_entries())} translations)")

if __name__ == "__main__":
    compile_locales()