"""fsspec adapter. Imported separately — requires fsspec."""

import fsspec
from fsspec.spec import AbstractFileSystem

from meret.vfs.tree import VirtualTree


class MeretFileSystem(AbstractFileSystem):
    protocol = "meret"

    def __init__(self, repo=None, **kw):
        super().__init__(**kw)
        self._tree = VirtualTree(repo)

    def ls(self, path, detail=True, **kw):
        nodes = self._tree.listdir(path)
        if not detail:
            return [n.name for n in nodes]
        return [
            {"name": n.vpath, "size": n.size, "type": "directory" if n.is_dir else "file"}
            for n in nodes
        ]

    def info(self, path, **kw):
        n = self._tree.stat(path)
        return {"name": n.vpath, "size": n.size, "type": "directory" if n.is_dir else "file"}

    def isdir(self, path):
        return self._tree.isdir(path)

    def exists(self, path, **kw):
        return self._tree.exists(path)

    def cat_file(self, path, start=None, end=None, **kw):
        if start is None and end is None:
            return self._tree.read(path)
        # only the requested range; negative bounds count from the end, as in slices
        with self._tree.open_source(path) as src:
            begin, stop, _ = slice(start, end).indices(src.size)
            return src.pread(begin, stop - begin)

    def _open(self, path, mode="rb", **kw):
        if mode != "rb":
            raise PermissionError(f"meret VFS is read-only: {path} ({mode})")
        return open(self._tree.real_path(path), "rb")  # a real file: streams, seeks


def register() -> None:
    """Registers the 'meret' protocol with fsspec."""
    fsspec.register_implementation("meret", MeretFileSystem)
