"""Meret virtual FS: tags as folders.

Public API. Backends (fsspec/fuse/winfsp) are NOT imported here,
so their dependencies aren't pulled in unnecessarily.
"""

from meret.vfs.errors import IsADirectory, NotADirectory, NotFound, VfsError
from meret.vfs.tree import Node, SourceFile, VirtualTree, sanitize

__all__ = [
    "IsADirectory",
    "Node",
    "NotADirectory",
    "NotFound",
    "SourceFile",
    "VfsError",
    "VirtualTree",
    "sanitize",
]
