"""Монтирование виртуальной ФС. Только чтение.

Linux/macOS — libfuse через fusepy.
Windows     — WinFsp через winfspy (см. заметку в конце).
"""

import errno
import platform
import stat
from pathlib import Path

from meret.vfs import IsADirectory, NotFound, VirtualTree


# ---------- Linux / macOS ----------

def _mount_fuse(tree: VirtualTree, mountpoint: str, foreground: bool) -> None:
    from fuse import FUSE, LoggingMixIn, Operations

    class MeretOperations(LoggingMixIn, Operations):
        def __init__(self) -> None:
            self._buffers: dict[int, bytes] = {}
            self._next_fh = 0

        def getattr(self, path, fh=None):
            try:
                node = tree.stat(path)
            except NotFound:
                raise OSError(errno.ENOENT, "no such node")

            is_dir = node.is_dir
            return {
                "st_mode": (stat.S_IFDIR | 0o555) if is_dir else (stat.S_IFREG | 0o444),
                "st_nlink": 2,
                "st_size": 0 if is_dir else node.size,
                "st_mtime": node.mtime,
                "st_ctime": node.mtime,
            }

        def readdir(self, path, fh):
            return [".", ".."] + [n.name for n in tree.listdir(path)]

        def open(self, path, flags):
            try:
                self._buffers[self._next_fh] = tree.read(path)
            except IsADirectory:
                raise OSError(errno.EISDIR, "is a directory")
            except NotFound:
                raise OSError(errno.ENOENT, "no such file")
            self._next_fh += 1
            return self._next_fh - 1

        def read(self, path, size, offset, fh):
            return self._buffers[fh][offset:offset + size]

        def release(self, path, fh):
            self._buffers.pop(fh, None)

        def access(self, path, mode):
            return True

    Path(mountpoint).mkdir(parents=True, exist_ok=True)
    FUSE(MeretOperations(), mountpoint, foreground=foreground, ro=True, nothreads=False)


# ---------- Windows ----------

def _mount_winfsp(tree: VirtualTree, mountpoint: str, foreground: bool) -> None:
    from winfspy import (
        FILE_ATTRIBUTE,
        NTStatusDirectoryNotEmpty,
        NTStatusEndOfFile,
        NTStatusNotADirectory,
        NTStatusObjectNameNotFound,
        BaseFileSystemOperations,
        FileSystem,
    )
    from winfspy.plumbing.security_descriptor import SecurityDescriptor
    _SD = SecurityDescriptor.from_string("O:BAG:BAD:P(A;;FA;;;SY)(A;;FA;;;BA)(A;;FA;;;WD)")

    class MeretOperations(BaseFileSystemOperations):
        def get_volume_info(self):
            return {"total_size": 0, "free_size": 0, "volume_label": "meret"}

        def get_security_by_name(self, file_name):
            try:
                node = tree.stat(_norm(file_name))
            except NotFound:
                raise NTStatusObjectNameNotFound()
            attrs = FILE_ATTRIBUTE.FILE_ATTRIBUTE_DIRECTORY if node.is_dir else 0
            return attrs, _SD.handle, _SD.size 

        def open(self, file_name, create_options, granted_access):
            try:
                node = tree.stat(_norm(file_name))
            except NotFound:
                raise NTStatusObjectNameNotFound()
            return 0, {"is_dir": node.is_dir, "size": node.size, "path": _norm(file_name)}

        def close(self, file_context):
            return None

        def get_file_info(self, file_context):
            node = tree.stat(file_context["path"])
            return {
                "file_attributes": (
                    FILE_ATTRIBUTE.FILE_ATTRIBUTE_DIRECTORY if node.is_dir else 0
                ),
                "file_size": 0 if node.is_dir else node.size,
                "allocation_size": 0,
                "creation_time": 0,
                "last_access_time": 0,
                "last_write_time": 0,
                "index_number": node.file_id or 0,
            }

        def read_directory(self, file_context, marker):
            if not file_context["is_dir"]:
                raise NTStatusNotADirectory()
            return [
                {
                    "file_name": n.name,
                    "file_attributes": (
                        FILE_ATTRIBUTE.FILE_ATTRIBUTE_DIRECTORY if n.is_dir else 0
                    ),
                    "file_size": 0 if n.is_dir else n.size,
                    "allocation_size": 0,
                    "creation_time": 0,
                    "last_access_time": 0,
                    "last_write_time": 0,
                    "index_number": n.file_id or 0,
                }
                for n in tree.listdir(file_context["path"])
            ]

        def read(self, file_context, offset, length):
            data = tree.read(file_context["path"])[offset:offset + length]
            if not data:
                raise NTStatusEndOfFile()
            return data

    def _norm(name: str) -> str:
        """Windows отдаёт '\\пляж\\file.png' — приводим к '/пляж/file.png'."""
        name = name.replace("\\", "/")
        return name if name.startswith("/") else "/" + name

    FileSystem(mountpoint, MeretOperations(), sector_size=4096, volume_label="meret")


# ---------- диспетчер ----------

def mount(tree: VirtualTree, mountpoint: str | Path, foreground: bool = True) -> None:
    """Смонтировать виртуальную ФС (только чтение)."""
    mountpoint = str(mountpoint)
    if platform.system() == "Windows":
        _mount_winfsp(tree, mountpoint, foreground)
    else:
        _mount_fuse(tree, mountpoint, foreground)