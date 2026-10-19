#!/usr/bin/env python3
"""Compile .po files to .mo files."""

import subprocess
from pathlib import Path

def compile_locales():
    locales_dir = Path(__file__).parent.parent / "meret" / "locales"
    
    for po_file in locales_dir.rglob("*.po"):
        mo_file = po_file.parent / "messages.mo"
        print(f"Compiling {po_file} -> {mo_file}")
        subprocess.run(["msgfmt", str(po_file), "-o", str(mo_file)], check=True)
        print(f"  ✓ Done")

if __name__ == "__main__":
    compile_locales()