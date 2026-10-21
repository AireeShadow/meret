# tests/gui/test_architecture.py
"""The GUI layer: Qt stays in meret/gui, and nothing below it depends on it."""
import ast
from pathlib import Path

MERET = Path(__file__).resolve().parents[2] / "meret"


def _imports(py: Path) -> set[str]:
    tree = ast.parse(py.read_text(encoding="utf-8"))
    mods: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            mods.update(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            mods.add(node.module)
    return mods


def test_only_gui_imports_qt():
    """core, vfs and cli must stay importable without PySide6."""
    for package in ("core", "vfs", "cli"):
        for py in (MERET / package).rglob("*.py"):
            bad = {m for m in _imports(py) if m.startswith("PySide6")}
            assert not bad, f"{py.name} imports {bad}"


def test_core_is_independent_of_gui():
    for py in (MERET / "core").rglob("*.py"):
        bad = {m for m in _imports(py) if m.startswith("meret.gui")}
        assert not bad, f"{py.name} imports {bad}"


def test_gui_does_not_depend_on_cli():
    """The GUI is a front-end, not a subcommand: no Typer, no rich."""
    for py in (MERET / "gui").rglob("*.py"):
        bad = {m for m in _imports(py) if m.startswith("meret.cli")}
        assert not bad, f"{py.name} imports {bad}"


def test_gui_does_not_import_backends():
    """`meret.gui` must not drag in FUSE or WinFsp."""
    for py in (MERET / "gui").rglob("*.py"):
        bad = {m for m in _imports(py) if m.startswith("meret.vfs.backends")}
        assert not bad, f"{py.name} imports {bad}"