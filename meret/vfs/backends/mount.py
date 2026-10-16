"""Mounting the virtual FS. Read-only.

Linux/macOS — libfuse via fusepy.
Windows     — WinFsp via winfspy.

winfspy contract (important!)
-----------------------------
``ll_open`` / ``ll_create`` do ``ffi.new_handle(cooked_file_context)`` —
so whatever ``open``/``create`` returns becomes the file context.
There is no tuple unpacking: return the context object ITSELF.

Stopping the mount (Windows)
----------------------------
``FileSystem.stop()`` calls ``FspFileSystemStopDispatcher``, which waits for
all IRPs issued by the driver to complete. While Explorer keeps a window open
on the drive letter, IRPs keep arriving and stop() can hang forever. So:

1. before stop() we remove the mount point (``FspFileSystemRemoveMountPoint``);
2. stop() runs in a separate thread; the main thread waits for it with a timeout;
3. a safety watchdog thread calls ``os._exit(0)`` unconditionally;
4. a second Ctrl+C (or Enter) exits immediately.
"""

import errno
import itertools
import os
import platform
import signal
import stat
import sys
import threading
import time
from pathlib import Path

from meret.vfs import IsADirectory, NotFound, SourceFile, VirtualTree

import logging

_DEBUG = bool(os.environ.get("MERET_DEBUG"))

# How long to wait for a clean stop before forcing an exit.
STOP_GRACE_SECONDS = 5.0

# Polling interval for the stop flag in the main thread.
POLL_INTERVAL = 0.2


def _flush_streams() -> None:
    """os._exit() doesn't flush buffers — do it manually."""
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.flush()
        except Exception:  # noqa: BLE001 — the stream may already be closed
            pass


def _install_signal_handlers(handler) -> None:
    """Ctrl+C and Ctrl+Break → a single handler."""
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
            # mount() was called from a non-main thread — signals aren't ours
            pass


def _watch_stdin(is_done, on_quit) -> None:
    """Enter in the terminal = exit.

    A fallback for when the console swallows Ctrl+C
    (QuickEdit in conhost, text selection in Windows Terminal).
    """
    try:
        while not is_done():
            line = sys.stdin.readline()
            if line == "":
                return  # stdin is closed — quietly exit the thread
            on_quit()
            return
    except Exception:  # noqa: BLE001
        return


# ---------- Linux / macOS ----------


def _mount_fuse(tree: VirtualTree, mountpoint: str, foreground: bool) -> None:
    from fuse import FUSE, LoggingMixIn, Operations

    class MeretOperations(LoggingMixIn, Operations):
        def __init__(self) -> None:
            # fh -> the real file; libfuse calls in from several threads
            self._files: dict[int, SourceFile] = {}
            self._fh = itertools.count()
            self._lock = threading.Lock()

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
                src = tree.open_source(path)
            except IsADirectory:
                raise OSError(errno.EISDIR, "is a directory")
            except NotFound:
                raise OSError(errno.ENOENT, "no such file")
            with self._lock:
                fh = next(self._fh)
                self._files[fh] = src
            return fh

        def read(self, path, size, offset, fh):
            # only the requested range: a thumbnailer peeking at a 4 GB video
            # must not pull the whole thing into memory
            with self._lock:
                src = self._files.get(fh)
            if src is None:
                raise OSError(errno.EBADF, "bad file handle")
            return src.pread(offset, size)

        def release(self, path, fh):
            with self._lock:
                src = self._files.pop(fh, None)
            if src is not None:
                src.close()
            return 0

        def access(self, path, mode):
            return 0

    Path(mountpoint).mkdir(parents=True, exist_ok=True)
    FUSE(MeretOperations(), mountpoint, foreground=foreground, ro=True, nothreads=False)


# ---------- Windows ----------


def _norm_windows_path(name: str) -> str:
    """WinFsp passes '\\beach\\file.png' — normalize to '/beach/file.png'."""
    name = name.replace("\\", "/")
    return name if name.startswith("/") else "/" + name


_FILETIME_EPOCH = 116444736000000000  # 1970-01-01 in 100 ns ticks since 1601


def _filetime(mtime: float) -> int:
    """Unix seconds → Windows FILETIME. 0 stays 0 (unknown)."""
    return int(mtime * 10_000_000) + _FILETIME_EPOCH if mtime > 0 else 0


def _mount_winfsp(tree: VirtualTree, mountpoint: str, foreground: bool) -> None:
    if _DEBUG:
        logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    from winfspy import (
        FILE_ATTRIBUTE,
        NTStatusEndOfFile,
        NTStatusMediaWriteProtected,
        NTStatusNotADirectory,
        NTStatusObjectNameNotFound,
        BaseFileSystemOperations,
        FileSystem,
    )
    from winfspy.plumbing import lib, ffi, NTSTATUS, NTStatusError
    from winfspy.plumbing.security_descriptor import SecurityDescriptor

    _SD = SecurityDescriptor.from_string("O:BAG:BAD:P(A;;FA;;;SY)(A;;FA;;;BA)(A;;FA;;;WD)")

    class MeretOperations(BaseFileSystemOperations):
        def __init__(self) -> None:
            super().__init__()
            # guards the lazy open of file_context["source"]: several threads
            # may read through one handle at once
            self._source_lock = threading.Lock()

        # ---------- debugging (MERET_DEBUG=1) ----------

        def _log(self, msg: str) -> None:
            if _DEBUG:
                print(f"[meret] {msg}", flush=True)

        @staticmethod
        def _addr(handle) -> int:
            return int(ffi.cast("size_t", handle))

        def ll_create(self, *args):
            status = super().ll_create(*args)
            self._log(f"ll_create -> status={status:#x} ctx={self._addr(args[-2][0]):#x}")
            return status

        def ll_open(self, *args):
            status = super().ll_open(*args)
            self._log(f"ll_open   -> status={status:#x} ctx={self._addr(args[-2][0]):#x}")
            return status

        def ll_close(self, file_context):
            """Our own ll_close instead of winfspy's.

            winfspy does `del self._opened_objs[file_context]` — that is the
            only reference to the cdata handle, so it gets freed. But WinFsp
            may call Close again (and from different threads) → from_handle
            reads freed memory → Py_FatalError. On top of that, the `del` itself
            races with a concurrent Close → KeyError.

            So we never remove entries: `_opened_objs` is a registry of every handle
            ever issued, and they live until the process exits.
            """
            if file_context not in self._opened_objs:
                # a pointer we don't know — let winfspy decide
                return super().ll_close(file_context)

            self._log(f"ll_close  <- ptr={self._addr(file_context):#x}")
            try:
                self.close(ffi.from_handle(file_context))
            except NTStatusError as exc:
                return exc.value
            except Exception:  # noqa: BLE001
                return NTSTATUS.STATUS_UNEXPECTED_IO_ERROR
            return NTSTATUS.STATUS_SUCCESS

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

        def get_security(self, file_context):
            # Without this implementation winfspy raises NotImplementedError on QuerySecurity.
            return _SD.handle, _SD.size

        def _context(self, vpath: str) -> dict:
            """Context of an open handle. Return IT, not a tuple."""
            node = tree.stat(vpath)
            return {
                "is_dir": node.is_dir,
                "size": node.size,
                "path": vpath,
                "source": None,  # the real file, opened on the first read
            }

        def _source(self, file_context) -> SourceFile:
            with self._source_lock:
                src = file_context["source"]
                if src is None:
                    try:
                        src = tree.open_source(file_context["path"])
                    except NotFound:
                        raise NTStatusObjectNameNotFound()
                    file_context["source"] = src
                return src

        def open(self, file_name, create_options, granted_access):
            vpath = _norm_windows_path(file_name)
            try:
                return self._context(vpath)
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
            """WinFsp calls create even for existing files.

            If the file exists, return its context. If not, we can't create it: we're read-only.
            """
            vpath = _norm_windows_path(file_name)
            try:
                return self._context(vpath)
            except NotFound:
                raise NTStatusMediaWriteProtected()

        def close(self, file_context):
            if isinstance(file_context, dict):
                with self._source_lock:
                    src, file_context["source"] = file_context.get("source"), None
                if src is not None:
                    src.close()
            return None

        def cleanup(self, file_context, file_name, flags):
            # Called when a handle is closed. We're read-only — nothing to do.
            return None

        def flush(self, file_context):
            return None

        def _info(self, node):
            mtime = _filetime(node.mtime)
            return {
                "file_attributes": (FILE_ATTRIBUTE.FILE_ATTRIBUTE_DIRECTORY if node.is_dir else 0),
                "allocation_size": 0,
                "file_size": 0 if node.is_dir else node.size,
                "creation_time": mtime,
                "last_access_time": mtime,
                "last_write_time": mtime,
                "change_time": mtime,
                "index_number": node.file_id or 0,
            }

        def get_file_info(self, file_context):
            try:
                node = tree.stat(file_context["path"])
            except NotFound:
                raise NTStatusObjectNameNotFound()
            return self._info(node)

        def read_directory(self, file_context, marker):
            if not file_context["is_dir"]:
                raise NTStatusNotADirectory()

            path = file_context["path"]
            entries = []

            # "." and ".." only for non-root directories
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
                    return entries[i + 1 :]
            return []

        def read(self, file_context, offset, length):
            # Explorer reads in 64 KB chunks: keep the real file open for the
            # lifetime of the handle and read only the requested range
            src = self._source(file_context)
            if offset >= src.size:
                raise NTStatusEndOfFile()
            return src.pread(offset, length)

    fs = FileSystem(
        mountpoint,
        MeretOperations(),
        sector_size=512,
        sectors_per_allocation_unit=1,
        file_info_timeout=1000,
        case_sensitive_search=1,
        case_preserved_names=1,  # mandatory — otherwise Cyrillic names get mangled
        unicode_on_disk=1,
        post_cleanup_when_modified_only=1,
        file_system_name=mountpoint,
    )

    shutdown = threading.Event()
    stopped = threading.Event()

    def _detach_mount_point() -> None:
        """Remove the drive letter BEFORE stopping the dispatcher.

        Explorer loses the volume and stops sending new IRPs — then
        StopDispatcher can wait for the in-flight ones instead of waiting forever.
        """
        ptr = getattr(fs, "_file_system_ptr", None)
        if ptr is None:
            return
        try:
            lib.FspFileSystemRemoveMountPoint(ptr[0])
        except Exception as exc:  # noqa: BLE001
            print(f"meret: failed to remove the mount point: {exc!r}", file=sys.stderr)

    def _stop() -> None:
        _detach_mount_point()
        try:
            fs.stop()
        except Exception as exc:  # noqa: BLE001
            print(f"meret: error while stopping the FS: {exc!r}", file=sys.stderr)
        finally:
            stopped.set()

    def _start_watchdog() -> None:
        """Guaranteed exit: even if the main thread is stuck, we die."""

        def _watchdog() -> None:
            if not stopped.wait(STOP_GRACE_SECONDS):
                print(
                    f"\nmeret: FS did not stop within {STOP_GRACE_SECONDS:g} s — forcing exit",
                    file=sys.stderr,
                )
            _flush_streams()
            os._exit(0)

        threading.Thread(target=_watchdog, name="meret-watchdog", daemon=True).start()

    def _request_shutdown(reason: str) -> None:
        if shutdown.is_set():
            print(f"\nmeret: {reason} — forcing exit")
            _flush_streams()
            os._exit(0)

        print(
            f"\nmeret: {reason} — stopping the FS (Ctrl+C or Enter again to exit immediately)",
            flush=True,
        )
        shutdown.set()
        threading.Thread(target=_stop, name="meret-stop", daemon=True).start()
        _start_watchdog()

    try:
        fs.start()
    except KeyboardInterrupt:
        # A signal arrived during startup — our handlers aren't installed yet.
        _flush_streams()
        try:
            fs.stop()
        except Exception:  # noqa: BLE001
            pass
        os._exit(0)
    except BaseException:
        # Startup failed: the dispatcher may already be running — clean up after ourselves.
        _flush_streams()
        try:
            fs.stop()
        except Exception:  # noqa: BLE001
            pass
        raise

    _install_signal_handlers(lambda signum, frame: _request_shutdown("signal received"))

    # Enter in the terminal is a second exit path, independent of signals.
    if sys.stdin is not None and sys.stdin.isatty():
        threading.Thread(
            target=_watch_stdin,
            args=(shutdown.is_set, lambda: _request_shutdown("Enter pressed")),
            name="meret-stdin",
            daemon=True,
        ).start()

    try:
        while not shutdown.is_set():
            time.sleep(POLL_INTERVAL)

        if not stopped.wait(STOP_GRACE_SECONDS):
            print(f"meret: FS did not stop within {STOP_GRACE_SECONDS:g} s — forcing exit")
    finally:
        _flush_streams()
        os._exit(0)


# ---------- dispatcher ----------


def mount(tree: VirtualTree, mountpoint: str | Path, foreground: bool = True) -> None:
    """Mount the virtual FS (read-only)."""
    mountpoint = str(mountpoint)
    if platform.system() == "Windows":
        _mount_winfsp(tree, mountpoint, foreground)
    else:
        _mount_fuse(tree, mountpoint, foreground)
