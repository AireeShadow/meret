"""Виртуальная ФС Meret: теги как папки.

Публичный API. Бэкенды (fsspec/fuse/winfsp) сюда НЕ импортируются,
чтобы не тянуть их зависимости без нужды.
"""
from meret.vfs.errors import IsADirectory, NotADirectory, NotFound, VfsError
from meret.vfs.tree import Node, VirtualTree, sanitize

__all__ = [
    "VirtualTree",
    "Node",
    "sanitize",
    "VfsError",
    "NotFound",
    "NotADirectory",
    "IsADirectory",
]