"""meret's graphical front-end (PySide6).

Qt lives here and nowhere else, so `core`, `vfs` and `cli` stay importable
without it. Importing this package pulls PySide6 in.
"""