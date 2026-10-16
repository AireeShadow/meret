"""In-memory AbstractRepository implementation for tests.

Lives in tests, but can easily move to meret/core/ if needed.
"""

import functools
from typing import Dict, List, Optional, Set, Tuple

from meret.core import tagpaths
from meret.core.models import FileEntry, FileType, Tag, TagNode
from meret.core.repository import AbstractRepository



def _mutates(method):
    """Bumps data_version after a write — like a commit in SQLite."""

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        finally:
            self._version += 1

    return wrapper


class MockRepository(AbstractRepository):
    """In-memory repository. Mirrors the semantics of the SQLite version."""

    def __init__(self) -> None:
        self._files: Dict[int, FileEntry] = {}
        self._tags: Dict[int, Tag] = {}
        self._links: Set[Tuple[int, int]] = set()
        self._next_file_id = 1
        self._next_tag_id = 1
        self._version = 0

    # ---------- helpers ----------
    def _child(self, parent_id: Optional[int], chunk: str) -> Optional[Tag]:
        key = chunk.casefold()
        for tag in self._tags.values():
            if tag.parent_id == parent_id and tag.name.casefold() == key:
                return tag
        return None

    def _tag_strict(self, ref: str) -> Optional[Tag]:
        chunks = tagpaths.parts(ref)
        if not chunks:
            return None
        tag = None
        parent_id: Optional[int] = None
        for chunk in chunks:
            tag = self._child(parent_id, chunk)
            if tag is None:
                return None
            parent_id = tag.id
        return tag

    def _tag_by(self, ref: str) -> Optional[Tag]:
        chunks = tagpaths.parts(ref)
        if not chunks:
            return None

        tag = self._tag_strict(ref)
        if tag is not None or len(chunks) > 1:
            return tag

        matches = [t for t in self._tags.values() if t.name.casefold() == chunks[0].casefold()]
        matches.sort(key=lambda t: (t.parent_id is not None, t.id or 0))
        return matches[0] if matches else None

    def _ensure_tag(self, ref: str, colour: Optional[str] = None, reuse_any: bool = True) -> int:
        chunks = tagpaths.parts(ref)
        if not chunks:
            raise ValueError(f"empty tag path: {ref!r}")

        if reuse_any and len(chunks) == 1:
            existing = self._tag_by(chunks[0])
            if existing:
                return existing.id

        parent_id: Optional[int] = None
        tag_id: Optional[int] = None
        for i, chunk in enumerate(chunks):
            tag = self._child(parent_id, chunk)
            if tag is None:
                tag = Tag(
                    id=self._next_tag_id,
                    name=chunk,
                    colour=colour if i == len(chunks) - 1 else None,
                    parent_id=parent_id,
                )
                self._tags[tag.id] = tag
                self._next_tag_id += 1
            tag_id = tag.id
            parent_id = tag_id
        return tag_id

    def _tag_names(self, file_id: int) -> List[str]:
        names = [self._tags[t].name for (f, t) in self._links if f == file_id]
        return sorted(names, key=str.casefold)

    def _usage(self, tag_id: int) -> int:
        return sum(1 for (_, t) in self._links if t == tag_id)

    def _clone(self, entry: FileEntry) -> FileEntry:
        clone = entry.model_copy(deep=True)
        clone.labels = self._tag_names(entry.id)
        return clone

    # ---------- files ----------
    def get_file_by_id(self, file_id: int) -> Optional[FileEntry]:
        entry = self._files.get(file_id)
        return self._clone(entry) if entry else None

    def get_file_by_path(self, path: str) -> Optional[FileEntry]:
        for entry in self._files.values():
            if entry.filepath == path:
                return self._clone(entry)
        return None

    @_mutates
    def save_file(self, file: FileEntry) -> int:
        for entry in self._files.values():
            if entry.filepath == file.filepath:
                entry.filename = file.filename
                entry.rating = file.rating
                entry.type = file.type
                entry.metadata = file.metadata
                file.id = entry.id
                return entry.id
        file.id = self._next_file_id
        self._next_file_id += 1
        self._files[file.id] = file.model_copy(deep=True)
        return file.id

    @_mutates
    def save_files(self, files, chunk_size: int = 1000, on_progress=None) -> Dict[str, int]:
        stats = {"indexed": 0, "updated": 0, "errors": 0}
        total = len(files)
        for i, entry in enumerate(files, start=1):
            existed = self.get_file_by_path(entry.filepath) is not None
            self.save_file(entry)
            stats["updated" if existed else "indexed"] += 1
            if on_progress and (i % chunk_size == 0 or i == total):
                on_progress(i, total)
        return stats

    @_mutates
    def delete_file(self, file_id: int) -> bool:
        if file_id not in self._files:
            return False
        del self._files[file_id]
        self._links = {(f, t) for (f, t) in self._links if f != file_id}
        return True

    # ---------- tags: file links ----------
    @_mutates
    def add_tag(self, file_id: int, tag: str, colour: Optional[str] = None) -> int:
        tag_id = self._ensure_tag(tag, colour)
        self._links.add((file_id, tag_id))
        return tag_id

    @_mutates
    def remove_tag(self, file_id: int, tag: str) -> bool:
        found = self._tag_by(tag)
        if not found or (file_id, found.id) not in self._links:
            return False
        self._links.discard((file_id, found.id))
        return True

    @_mutates
    def set_file_tags(self, file_id: int, tags: List[str]) -> bool:
        self._links = {(f, t) for (f, t) in self._links if f != file_id}
        for name in dict.fromkeys(tags):
            self._links.add((file_id, self._ensure_tag(name)))
        return True

    @_mutates
    def rename_tag(self, old: str, new: str) -> bool:
        src = self._tag_by(old)
        if not src:
            return False
        chunks = tagpaths.parts(new)
        if not chunks:
            return False
        new_name = chunks[-1]

        dst = self._child(src.parent_id, new_name)
        if dst and dst.id != src.id:
            for f, t in list(self._links):
                if t == src.id:
                    self._links.add((f, dst.id))
            self._links = {(f, t) for (f, t) in self._links if t != src.id}
            for other in self._tags.values():
                if other.parent_id == src.id:
                    other.parent_id = src.parent_id
            del self._tags[src.id]
        else:
            src.name = new_name
        return True

    # ---------- tags: creation and hierarchy ----------
    def _would_cycle(self, tag_id: int, parent_id: int) -> bool:
        """True if parent_id is tag_id itself or one of its descendants.

        Walk up FROM THE PARENT looking for the tag: if we hit it, the parent is inside
        the tag's subtree and reparenting would create a loop.
        """
        seen: Set[int] = set()
        cur: Optional[int] = parent_id
        while cur is not None and cur not in seen:
            if cur == tag_id:
                return True
            seen.add(cur)
            tag = self._tags.get(cur)
            cur = tag.parent_id if tag else None
        return False

    @_mutates
    def create_tag(self, name: str, colour: Optional[str] = None) -> bool:
        if self._tag_strict(name) is not None:
            return False
        self._ensure_tag(name, colour, reuse_any=False)
        return True

    @_mutates
    def delete_tag(self, name: str) -> bool:
        tag = self._tag_by(name)
        if not tag:
            return False
        for other in self._tags.values():
            if other.parent_id == tag.id:
                other.parent_id = tag.parent_id
        del self._tags[tag.id]
        self._links = {(f, t) for (f, t) in self._links if t != tag.id}
        return True

    @_mutates
    def set_tag_parent(self, tag: str, parent: Optional[str]) -> bool:
        node = self._tag_by(tag)
        if not node:
            return False
        if parent is None:
            node.parent_id = None
            return True
        parent_tag = self._tag_by(parent)
        if not parent_tag or self._would_cycle(node.id, parent_tag.id):
            return False
        node.parent_id = parent_tag.id
        return True

    # ---------- tags: reading ----------
    def get_all_tags(self) -> List[Tag]:
        return sorted(
            (t.model_copy() for t in self._tags.values()),
            key=lambda t: t.name.casefold(),
        )

    def get_tag_usage(self, limit: int = 100) -> List[Tag]:
        tags = []
        for tag in self._tags.values():
            clone = tag.model_copy()
            clone.usage_count = self._usage(tag.id)
            tags.append(clone)
        tags.sort(key=lambda t: (-t.usage_count, t.name.casefold()))
        return tags[:limit]

    def get_tag_tree(self) -> List[TagNode]:
        nodes = {
            t.id: TagNode(**{**t.model_dump(), "usage_count": self._usage(t.id)})
            for t in self._tags.values()
        }
        roots: List[TagNode] = []
        for node in nodes.values():
            parent = nodes.get(node.parent_id)
            if parent is None or parent is node:
                roots.append(node)
            else:
                parent.children.append(node)
        return roots

    @_mutates
    def prune_orphan_tags(self) -> int:
        parents = {t.parent_id for t in self._tags.values() if t.parent_id}
        orphans = [tid for tid in self._tags if self._usage(tid) == 0 and tid not in parents]
        for tid in orphans:
            del self._tags[tid]
        for tag in self._tags.values():
            if tag.parent_id in orphans:
                tag.parent_id = None
        return len(orphans)

    # ---------- search ----------
    def search_by_tags(self, tags, match_all=False, file_type=None, limit=100):
        wanted = {t.casefold() for t in tags}
        results = []
        for entry in self._files.values():
            if file_type and entry.type.value != file_type:
                continue
            names = {n.casefold() for n in self._tag_names(entry.id)}
            if not wanted:
                ok = True
            elif match_all:
                ok = wanted <= names
            else:
                ok = bool(wanted & names)
            if ok:
                results.append(self._clone(entry))
        return results[:limit]

    def search_by_tag_ids(self, groups, match_all=False, file_type=None, limit=None):
        wanted = [set(g) for g in groups if g]
        if not wanted:
            return []
        results = []
        for entry in self._files.values():
            if file_type and entry.type.value != file_type:
                continue
            mine = {t for (f, t) in self._links if f == entry.id}
            hits = [bool(g & mine) for g in wanted]
            if all(hits) if match_all else any(hits):
                results.append(self._clone(entry))
        return results[:limit] if limit is not None else results

    def search_by_name(self, query: str, limit: int = 100) -> List[FileEntry]:
        q = query.casefold()
        hits = [self._clone(e) for e in self._files.values() if q in e.filename.casefold()]
        return hits[:limit]

    def get_stats(self) -> dict:
        by_type: Dict[str, int] = {}
        for entry in self._files.values():
            by_type[entry.type.value] = by_type.get(entry.type.value, 0) + 1
        return {
            "total_files": len(self._files),
            "by_type": by_type,
            "total_tags": len(self._tags),
        }

    def known_paths(self) -> Set[str]:
        return {entry.filepath for entry in self._files.values()}

    @staticmethod
    def _fingerprint(entry: FileEntry) -> Tuple[int, float]:
        meta = entry.metadata or {}
        return int(meta.get("size_bytes") or 0), float(meta.get("modified_at") or 0.0)

    def known_fingerprints(self) -> Dict[str, Tuple[int, float]]:
        return {e.filepath: self._fingerprint(e) for e in self._files.values()}

    def data_version(self) -> Optional[int]:
        return self._version
