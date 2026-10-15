import posixpath
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple

from meret.core import tagpaths
from meret.core.models import FileEntry, Tag
from meret.core.repository import AbstractRepository
from meret.vfs.errors import IsADirectory, NotFound

_FORBIDDEN = '/\\:*?"<>|'
_SAFE_SUB = "∕"  # U+2215 DIVISION SLASH

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


class VirtualTree:
    """Maps the tag hierarchy onto virtual paths. Read-only."""

    def __init__(self, repo: AbstractRepository) -> None:
        self.repo = repo

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

    # ---------- tag index ----------

    def _tag_index(
        self, tags: Optional[List[Tag]] = None
    ) -> Dict[Tuple[Optional[int], str], Tag]:
        """{(parent_id, lowercase_name): tag} — for fast lookup.

        The index is built by tagpaths — one implementation for the whole project.
        The only difference is the key: VFS compares by sanitize(), not the raw name.
        tags may be passed in ready-made to avoid an extra repository call.
        """
        if tags is None:
            tags = self.repo.get_all_tags()
        return tagpaths.index_by_parent(tags, key=lambda t: sanitize(t.name).casefold())

    def _children_of(
        self, parent_id: Optional[int], tags: Optional[List[Tag]] = None
    ) -> List[Tag]:
        if tags is None:
            tags = self.repo.get_all_tags()
        return [t for t in tags if t.parent_id == parent_id]

    def _resolve_tag(
        self, vpath: str, tags: Optional[List[Tag]] = None
    ) -> Optional[Tag]:
        """Finds a tag by path. None for the root. NotFound if missing."""
        parts = self._parts(vpath)
        if not parts:
            return None

        index = self._tag_index(tags)
        parent_id: Optional[int] = None
        tag: Optional[Tag] = None
        for part in parts:
            tag = index.get((parent_id, part.casefold()))
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
    def _file_size(entry: FileEntry) -> int:
        """Size from metadata, otherwise from disk."""
        size = (entry.metadata or {}).get("size_bytes")
        if size:
            return int(size)
        try:
            return Path(entry.filepath).stat().st_size
        except OSError:
            return 0

    # ---------- listing ----------

    def listdir(self, vpath: str = "/") -> List[Node]:
        """Returns the directory contents: subtags (folders) + files."""
        vpath = self.normalize(vpath)
        tags = self.repo.get_all_tags()
        tag = self._resolve_tag(vpath, tags)

        # subdirectories = child tags
        child_tags = sorted(
            self._children_of(tag.id if tag else None, tags),
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

        # files = files tagged with this tag
        if tag is not None:
            files = self.repo.search_by_tags([tag.name])
            file_names = self._dedupe([f.filename for f in files])
            for name, f in zip(file_names, files):
                nodes.append(
                    Node(
                        name=name,
                        vpath=posixpath.join(vpath, name),
                        is_dir=False,
                        size=(f.metadata or {}).get("size_bytes", 0),
                        file_id=f.id,
                        source_path=f.filepath,
                    )
                )
        return nodes

    # ---------- stat / read ----------

    def stat(self, vpath: str) -> Node:
        vpath = self.normalize(vpath)
        if vpath == "/":
            return Node(name="/", vpath="/", is_dir=True)

        parent = posixpath.dirname(vpath) or "/"
        name = posixpath.basename(vpath)

        try:
            tag = self._resolve_tag(vpath)
            if tag is not None:
                return Node(name=sanitize(tag.name), vpath=vpath, is_dir=True, tag_id=tag.id)
        except NotFound:
            pass

        for node in self.listdir(parent):
            if node.name == name:
                return node
        raise NotFound(vpath)

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

    def walk(self, vpath: str = "/") -> Iterator[Node]:
        """Walks the whole tree."""
        for node in self.listdir(vpath):
            yield node
            if node.is_dir:
                yield from self.walk(node.vpath)

