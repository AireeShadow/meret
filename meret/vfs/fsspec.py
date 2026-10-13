"""Адаптер fsspec. Импортируется отдельно — требует fsspec."""
import io

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
            {"name": n.vpath, "size": n.size,
             "type": "directory" if n.is_dir else "file"}
            for n in nodes
        ]

    def info(self, path, **kw):
        n = self._tree.stat(path)
        return {"name": n.vpath, "size": n.size,
                "type": "directory" if n.is_dir else "file"}

    def isdir(self, path):
        return self._tree.isdir(path)

    def exists(self, path, **kw):
        return self._tree.exists(path)

    def cat_file(self, path, start=None, end=None, **kw):
        return self._tree.read(path)[start:end]

    def _open(self, path, mode="rb", **kw):
        return io.BytesIO(self._tree.read(path))


def register() -> None:
    """Регистрирует протокол 'meret' в fsspec."""
    fsspec.register_implementation("meret", MeretFileSystem)