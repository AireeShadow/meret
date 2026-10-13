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
            return {
                "total_size": 1 << 40,
                "free_size": 1 << 39,
                "volume_label": "meret",
            }

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

            path = file_context["path"]

            def _info(node):
                return {
                    "file_attributes": (
                        FILE_ATTRIBUTE.FILE_ATTRIBUTE_DIRECTORY if node.is_dir else 0
                    ),
                    "allocation_size": 0,
                    "file_size": 0 if node.is_dir else node.size,
                    "creation_time": 0,
                    "last_access_time": 0,
                    "last_write_time": 0,
                    "change_time": 0,
                    "index_number": node.file_id or 0,
                }

            entries = []

            # "." и ".." только для не-корня
            if path != "/":
                entries.append({"file_name": ".", **_info(tree.stat(path))})
                parent = path.rsplit("/", 1)[0] or "/"
                entries.append({"file_name": "..", **_info(tree.stat(parent))})

            entries += [
                {"file_name": n.name, **_info(n)} for n in tree.listdir(path)
            ]
            entries.sort(key=lambda e: e["file_name"])

            if marker is None:
                return entries

            for i, entry in enumerate(entries):
                if entry["file_name"] == marker:
                    return entries[i + 1:]
            return []

        def read(self, file_context, offset, length):
            data = tree.read(file_context["path"])
            if offset >= len(data):
                raise NTStatusEndOfFile()
            return data[offset:offset + length]

    def _norm(name: str) -> str:
        """Windows отдаёт '\\пляж\\file.png' — приводим к '/пляж/file.png'."""
        name = name.replace("\\", "/")
        return name if name.startswith("/") else "/" + name

    FileSystem(
        mountpoint,
        MeretOperations(),
        sector_size=512,
        sectors_per_allocation_unit=1,
        file_info_timeout=1000,
        case_sensitive_search=1,
        case_preserved_names=1,      # обязательно — иначе кириллица поедет
        unicode_on_disk=1,
        post_cleanup_when_modified_only=1,
        file_system_name=mountpoint,
    )


# ---------- диспетчер ----------

def mount(tree: VirtualTree, mountpoint: str | Path, foreground: bool = True) -> None:
    """Смонтировать виртуальную ФС (только чтение)."""
    mountpoint = str(mountpoint)
    if platform.system() == "Windows":
        _mount_winfsp(tree, mountpoint, foreground)
    else:
        _mount_fuse(tree, mountpoint, foreground)