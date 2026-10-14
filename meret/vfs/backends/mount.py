"""Монтирование виртуальной ФС. Только чтение.

Linux/macOS — libfuse через fusepy.
Windows     — WinFsp через winfspy.

Остановка монтирования (Windows)
--------------------------------
``FileSystem.stop()`` вызывает ``FspFileSystemStopDispatcher``, который ждёт
завершения всех выданных драйвером IRP. Пока Проводник держит окно с буквой
диска, IRP приходят непрерывно, и stop() может висеть бесконечно. Поэтому:

1. перед stop() снимаем точку монтирования (``FspFileSystemRemoveMountPoint``)
   — Проводник теряет том и перестаёт слать новые запросы;
2. stop() выполняется в отдельном потоке, главный поток ждёт его с таймаутом
   (значит, главный поток не держит GIL внутри C-вызова);
3. повторный Ctrl+C — принудительный ``os._exit(0)``.

У winfspy нет демонизации: ``start()`` возвращает управление сразу, поэтому
ожидание сигнала здесь всегда блокирующее, независимо от ``foreground``.
"""

import errno
import os
import platform
import signal
import stat
import sys
import threading
import time
from pathlib import Path

from meret.vfs import IsADirectory, NotFound, VirtualTree

# Сколько ждём корректной остановки, прежде чем выйти принудительно.
STOP_GRACE_SECONDS = 5.0

# Период опроса флага остановки в главном потоке.
POLL_INTERVAL = 0.2


def _flush_streams() -> None:
    """os._exit() не сбрасывает буферы — делаем это руками."""
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.flush()
        except Exception:  # noqa: BLE001 — поток мог быть уже закрыт
            pass


def _install_signal_handlers(handler) -> None:
    """Ctrl+C и Ctrl+Break → один обработчик."""
    names = ["SIGINT"]
    if platform.system() == "Windows":
        names.append("SIGBREAK")

    for name in names:
        sig = getattr(signal, name, None)
        if sig is None:
            continue
        try:
            signal.signal(sig, handler)
        except ValueError:
            # mount() позвали не из главного потока — сигналы не наши
            pass


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

def _norm_windows_path(name: str) -> str:
    """WinFsp отдаёт '\\пляж\\file.png' — приводим к '/пляж/file.png'."""
    name = name.replace("\\", "/")
    return name if name.startswith("/") else "/" + name


def _mount_winfsp(tree: VirtualTree, mountpoint: str, foreground: bool) -> None:
    from winfspy import (
        FILE_ATTRIBUTE,
        NTStatusEndOfFile,
        NTStatusMediaWriteProtected,
        NTStatusNotADirectory,
        NTStatusObjectNameNotFound,
        BaseFileSystemOperations,
        FileSystem,
    )
    from winfspy.plumbing import lib
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
                node = tree.stat(_norm_windows_path(file_name))
            except NotFound:
                raise NTStatusObjectNameNotFound()
            attrs = FILE_ATTRIBUTE.FILE_ATTRIBUTE_DIRECTORY if node.is_dir else 0
            return attrs, _SD.handle, _SD.size

        def _context(self, vpath: str) -> dict:
            node = tree.stat(vpath)
            return {"is_dir": node.is_dir, "size": node.size, "path": vpath}

        def open(self, file_name, create_options, granted_access):
            vpath = _norm_windows_path(file_name)
            try:
                return 0, self._context(vpath)
            except NotFound:
                raise NTStatusObjectNameNotFound()

        def create(
            self,
            file_name,
            create_options,
            granted_access,
            file_attributes,
            security_descriptor,
            allocation_size,
        ):
            """WinFsp зовёт create даже для существующих файлов.

            Файл есть — отдаём контекст. Нет — создать не можем, мы read-only.
            """
            vpath = _norm_windows_path(file_name)
            try:
                return 0, self._context(vpath)
            except NotFound:
                raise NTStatusMediaWriteProtected()

        def close(self, file_context):
            return None

        def _info(self, node):
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

        def get_file_info(self, file_context):
            return self._info(tree.stat(file_context["path"]))

        def read_directory(self, file_context, marker):
            if not file_context["is_dir"]:
                raise NTStatusNotADirectory()

            path = file_context["path"]
            entries = []

            # "." и ".." только для не-корня
            if path != "/":
                entries.append({"file_name": ".", **self._info(tree.stat(path))})
                parent = path.rsplit("/", 1)[0] or "/"
                entries.append({"file_name": "..", **self._info(tree.stat(parent))})

            entries += [{"file_name": n.name, **self._info(n)} for n in tree.listdir(path)]
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

    fs = FileSystem(
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

    shutdown = threading.Event()
    stopped = threading.Event()

    def _detach_mount_point() -> None:
        """Снять букву диска ДО остановки диспетчера.

        Проводник теряет том и перестаёт присылать новые IRP — тогда
        StopDispatcher успевает дождаться уже начатых вместо вечного ожидания.
        """
        ptr = getattr(fs, "_file_system_ptr", None)
        if ptr is None:
            return
        try:
            lib.FspFileSystemRemoveMountPoint(ptr[0])
        except Exception as exc:  # noqa: BLE001
            print(f"meret: не удалось снять точку монтирования: {exc!r}", file=sys.stderr)

    def _stop() -> None:
        _detach_mount_point()
        try:
            fs.stop()
        except Exception as exc:  # noqa: BLE001
            print(f"meret: ошибка при остановке ФС: {exc!r}", file=sys.stderr)
        finally:
            stopped.set()

    def _on_signal(signum, frame) -> None:
        if shutdown.is_set():
            print("\nmeret: выхожу принудительно")
            _flush_streams()
            os._exit(0)

        shutdown.set()
        print("\nmeret: останавливаю ФС… (Ctrl+C ещё раз — выйти немедленно)")
        threading.Thread(target=_stop, name="meret-stop", daemon=True).start()

    try:
        fs.start()
    except KeyboardInterrupt:
        # Сигнал прилетел во время старта — там ещё нет наших хендлеров.
        _flush_streams()
        try:
            fs.stop()
        except Exception:  # noqa: BLE001
            pass
        os._exit(0)

    _install_signal_handlers(_on_signal)

    try:
        while not shutdown.is_set():
            time.sleep(POLL_INTERVAL)

        if not stopped.wait(STOP_GRACE_SECONDS):
            print(
                f"meret: ФС не остановилась за {STOP_GRACE_SECONDS:g} с — "
                "выхожу принудительно"
            )
    finally:
        _flush_streams()
        os._exit(0)


# ---------- диспетчер ----------

def mount(tree: VirtualTree, mountpoint: str | Path, foreground: bool = True) -> None:
    """Смонтировать виртуальную ФС (только чтение)."""
    mountpoint = str(mountpoint)
    if platform.system() == "Windows":
        _mount_winfsp(tree, mountpoint, foreground)
    else:
        _mount_fuse(tree, mountpoint, foreground)