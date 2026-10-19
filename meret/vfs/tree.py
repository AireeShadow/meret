import logging
import os
import posixpath
import threading
import time
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple

from meret.core import tagpaths
from meret.core.models import FileEntry, Tag
from meret.core.repository import AbstractRepository
from meret.vfs.errors import IsADirectory, NotFound

_FORBIDDEN = '/\\:*?"<>|'
_SAFE_SUB = "∕"  # U+2215 DIVISION SLASH

logger = logging.getLogger(__name__)

#: How long a snapshot lives when the repository can't report data_version().
DEFAULT_TTL = 1.0

_TagIndex = Dict[Tuple[Optional[int], str], Tag]


@dataclass(frozen=True)
class Node:
    name: str
    vpath: str
    is_dir: bool
    size: int = 0
    mtime: float = 0.0
    tag_id: Optional[int] = None
    file_id: Optional[int] = None
    source_path: Optional[str] = None  # real path on disk


def sanitize(name: str) -> str:
    """Makes a tag name safe for file systems."""
    name = unicodedata.normalize("NFC", name).strip()
    for ch in _FORBIDDEN:
        name = name.replace(ch, _SAFE_SUB)
    return name or "_"


class SourceFile:
    """The real file behind a virtual one, open for positional reads.

    Backends serve one handle from several threads, so reads never share a file
    position: os.pread where it exists (Linux/macOS), seek+read under a lock otherwise.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._file = open(self.path, "rb")
        self._lock = threading.Lock()

    @property
    def size(self) -> int:
        return os.fstat(self._file.fileno()).st_size

    def pread(self, offset: int, length: int) -> bytes:
        """Up to `length` bytes from `offset`. b"" at or past the end."""
        if length <= 0:
            return b""
        if hasattr(os, "pread"):
            with self._lock:
                return os.pread(self._file.fileno(), length, offset)
        with self._lock:
            self._file.seek(offset)
            return self._file.read(length)

    def close(self) -> None:
        """Idempotent: WinFsp may close the same handle twice."""
        with self._lock:
            self._file.close()

    def __enter__(self) -> "SourceFile":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


@dataclass
class _Listing:
    nodes: List[Node]
    by_name: Dict[str, Node]


@dataclass
class _Snapshot:
    """Everything the tree knows at one data_version. Replaced whole, never patched."""

    version: Optional[int]
    taken_at: float
    index: _TagIndex
    children: Dict[Optional[int], List[Tag]]
    listings: Dict[str, _Listing] = field(default_factory=dict)  # filled lazily


class VirtualTree:
    """Maps the tag hierarchy onto virtual paths. Read-only.

    Backends call stat() for every entry of every listing, so the tree keeps a
    snapshot: all tags plus each directory listing that was asked for. It is
    rebuilt when repo.data_version() moves; repositories that can't report it
    get a `ttl`-second snapshot instead.
    """

    def __init__(self, repo: AbstractRepository, ttl: float = DEFAULT_TTL) -> None:
        self.repo = repo
        self.ttl = ttl
        self._lock = threading.RLock()  # backends call in from many threads
        self._snap: Optional[_Snapshot] = None

    # ---------- path parsing ----------

    @staticmethod
    def normalize(vpath: str) -> str:
        if not vpath.startswith("/"):
            vpath = "/" + vpath
        vpath = posixpath.normpath(vpath)
        return "/" if vpath == "." else vpath

    @classmethod
    def _parts(cls, vpath: str) -> List[str]:
        norm = cls.normalize(vpath)
        return [p for p in norm.split("/") if p]

    # ---------- snapshot ----------

    def invalidate(self) -> None:
        """Drop the snapshot; the next call reads the repository again."""
        with self._lock:
            self._snap = None

    def _repo_version(self) -> Optional[int]:
        probe = getattr(self.repo, "data_version", None)
        return probe() if probe else None

    def _is_fresh(self, snap: _Snapshot, version: Optional[int]) -> bool:
        if version is not None:
            return snap.version == version
        return time.monotonic() - snap.taken_at < self.ttl

    def _snapshot(self) -> _Snapshot:
        with self._lock:
            # Fast path: return cached snapshot if still fresh
            snap = self._snap
            if snap is not None and self._is_fresh(snap, self._repo_version()):
                return snap
            
            # Rebuild snapshot with consistent data read
            started = time.perf_counter()
            previous = snap.version if snap is not None else None
            
            # Read all data first
            tags = self.repo.get_all_tags()
            children: Dict[Optional[int], List[Tag]] = {}
            for t in tags:
                children.setdefault(t.parent_id, []).append(t)
            
            # Read version AFTER data to detect concurrent modifications
            # If a write happened during our read, we'll rebuild next time
            version = self._repo_version()
            
            snap = _Snapshot(
                version=version,
                taken_at=time.monotonic(),
                index=self._tag_index(tags),
                children=children,
            )
            self._snap = snap
            
            logger.debug(
                "snapshot rebuilt: data_version %s -> %s, %d tags in %.1f ms",
                previous, version, len(tags),
                (time.perf_counter() - started) * 1000,
            )
            return snap

    # ---------- tag index ----------

    def _tag_index(self, tags: Optional[List[Tag]] = None) -> _TagIndex:
        """{(parent_id, lowercase_name): tag} — for fast lookup.

        The index is built by tagpaths — one implementation for the whole project.
        The only difference is the key: VFS compares by sanitize(), not the raw name.
        Without `tags` it is the current snapshot's index.
        """
        if tags is None:
            return self._snapshot().index
        return tagpaths.index_by_parent(tags, key=lambda t: sanitize(t.name).casefold())

    def _resolve_tag(self, vpath: str, snap: _Snapshot) -> Optional[Tag]:
        """Finds a tag by path. None for the root. NotFound if missing."""
        parent_id: Optional[int] = None
        tag: Optional[Tag] = None
        for part in self._parts(vpath):
            tag = snap.index.get((parent_id, part.casefold()))
            if tag is None:
                raise NotFound(vpath)
            parent_id = tag.id
        return tag

    # ---------- unique names ----------

    @staticmethod
    def _dedupe(names: List[str]) -> List[str]:
        """Disambiguates duplicate names with ~1, ~2 suffixes (stable)."""
        seen: Dict[str, int] = {}
        result: List[str] = []
        for name in names:
            key = name.casefold()
            if key not in seen:
                seen[key] = 0
                result.append(name)
                continue
            seen[key] += 1
            stem, dot, ext = name.rpartition(".")
            if dot:
                result.append(f"{stem}~{seen[key]}.{ext}")
            else:
                result.append(f"{name}~{seen[key]}")
        return result

    @staticmethod
    def _file_stat(entry: FileEntry) -> Tuple[int, float]:
        """(size, mtime) from metadata, otherwise from disk."""
        meta = entry.metadata or {}
        size, mtime = meta.get("size_bytes"), meta.get("modified_at")
        if size and mtime:
            return int(size), float(mtime)
        try:
            st = Path(entry.filepath).stat()
        except OSError:
            return int(size or 0), float(mtime or 0.0)
        return int(size or st.st_size), float(mtime or st.st_mtime)

    # ---------- listing ----------

    def _listing(self, vpath: str) -> _Listing:
        """Directory contents from the snapshot, read from the repository once."""
        with self._lock:
            snap = self._snapshot()
            listing = snap.listings.get(vpath)
            if listing is None:
                started = time.perf_counter()
                listing = self._build_listing(vpath, snap)
                snap.listings[vpath] = listing
                dirs = sum(1 for n in listing.nodes if n.is_dir)
                logger.debug(
                    "listing %s: %d folders, %d files in %.1f ms",
                    vpath, dirs, len(listing.nodes) - dirs, (time.perf_counter() - started) * 1000,
                )
            return listing

    def _build_listing(self, vpath: str, snap: _Snapshot) -> _Listing:
        tag = self._resolve_tag(vpath, snap)

        # subdirectories = child tags
        child_tags = sorted(
            snap.children.get(tag.id if tag else None, []),
            key=lambda t: sanitize(t.name).casefold(),
        )

        dir_names = self._dedupe([sanitize(t.name) for t in child_tags])
        nodes = [
            Node(
                name=name,
                vpath=posixpath.join(vpath, name) if vpath != "/" else "/" + name,
                is_dir=True,
                tag_id=t.id,
            )
            for name, t in zip(dir_names, child_tags)
        ]

        # files = files tagged with this tag (by id: namesakes are distinct)
        if tag is not None:
            files = self.repo.search_by_tag_ids([[tag.id]])
            file_names = self._dedupe([f.filename for f in files])
            for name, f in zip(file_names, files):
                size, mtime = self._file_stat(f)
                nodes.append(
                    Node(
                        name=name,
                        vpath=posixpath.join(vpath, name),
                        is_dir=False,
                        size=size,
                        mtime=mtime,
                        file_id=f.id,
                        source_path=f.filepath,
                    )
                )

        by_name: Dict[str, Node] = {}
        for node in nodes:
            by_name.setdefault(node.name, node)  # a folder wins over a same-named file
        return _Listing(nodes, by_name)

    def listdir(self, vpath: str = "/") -> List[Node]:
        """Returns the directory contents: subtags (folders) + files."""
        return list(self._listing(self.normalize(vpath)).nodes)

    # ---------- stat / read ----------

    def stat(self, vpath: str) -> Node:
        vpath = self.normalize(vpath)
        if vpath == "/":
            return Node(name="/", vpath="/", is_dir=True)

        try:
            tag = self._resolve_tag(vpath, self._snapshot())
            if tag is not None:
                return Node(name=sanitize(tag.name), vpath=vpath, is_dir=True, tag_id=tag.id)
        except NotFound:
            pass

        parent = posixpath.dirname(vpath) or "/"
        node = self._listing(parent).by_name.get(posixpath.basename(vpath))
        if node is None:
            raise NotFound(vpath)
        return node

    def exists(self, vpath: str) -> bool:
        try:
            self.stat(vpath)
            return True
        except NotFound:
            return False

    def isdir(self, vpath: str) -> bool:
        try:
            return self.stat(vpath).is_dir
        except NotFound:
            return False

    def read(self, vpath: str) -> bytes:
        """The whole file in memory. Backends use open_source() instead."""
        node = self.stat(vpath)
        if node.is_dir:
            raise IsADirectory(vpath)
        if not node.source_path:
            raise NotFound(vpath)
        return Path(node.source_path).read_bytes()

    def real_path(self, vpath: str) -> Path:
        """Path on disk — to hand the file to an external player."""
        node = self.stat(vpath)
        if node.is_dir or not node.source_path:
            raise IsADirectory(vpath)
        return Path(node.source_path)

    def open_source(self, vpath: str) -> SourceFile:
        """Opens the real file for ranged reads. The caller closes it.

        NotFound also when the file is in the database but gone from disk.
        """
        path = self.real_path(vpath)
        try:
            return SourceFile(path)
        except FileNotFoundError:
            logger.warning("in the database but missing on disk: %s -> %s", vpath, path)
            raise NotFound(vpath) from None

    def read_range(self, vpath: str, offset: int, length: int) -> bytes:
        """Up to `length` bytes from `offset` without reading the whole file."""
        with self.open_source(vpath) as src:
            return src.pread(offset, length)

    def walk(self, vpath: str = "/") -> Iterator[Node]:
        """Walks the whole tree."""
        for node in self.listdir(vpath):
            yield node
            if node.is_dir:
                yield from self.walk(node.vpath)
