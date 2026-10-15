# tests/vfs/test_architecture.py
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


def test_core_is_independent():
    """core knows nothing about vfs or cli."""
    for py in (MERET / "core").rglob("*.py"):
        bad = {m for m in _imports(py) if m.startswith(("meret.vfs", "meret.cli"))}
        assert not bad, f"{py.name} imports {bad}"


def test_vfs_tree_has_no_third_party():
    """The pure VFS core doesn't depend on fsspec/fuse/PIL."""
    mods = _imports(MERET / "vfs" / "tree.py")
    forbidden = {"fsspec", "fuse", "winfspy", "PIL"}
    assert not (mods & forbidden), f"tree.py imports {mods & forbidden}"


def test_vfs_init_does_not_import_backends():
    """import meret.vfs must not require fsspec."""
    mods = _imports(MERET / "vfs" / "__init__.py")
    assert not any(m.startswith("meret.vfs.backends") for m in mods)