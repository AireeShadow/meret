# meret 🐍

Tagged media library: tags instead of folders, virtual hierarchy, GUI.

## Installation
pip install -e ".[dev]"

## Quick start
meret init ~/Media
meret scan ~/Media --db ~/Media
meret tag ~/Media/photo.jpg vacation beach
meret search --tag vacation
meret tree