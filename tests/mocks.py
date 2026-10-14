"""In-memory реализация AbstractRepository для тестов.

Живёт в тестах, но при желании легко переезжает в meret/core/.
"""
from typing import Dict, List, Optional, Set, Tuple

from meret.core.models import FileEntry, FileType, Tag, TagNode
from meret.core.repository import AbstractRepository


class MockRepository(AbstractRepository):
    """Репозиторий в памяти. Повторяет семантику SQLite-версии."""

    def __init__(self) -> None:
        self._files: Dict[int, FileEntry] = {}
        self._tags: Dict[int, Tag] = {}
        self._links: Set[Tuple[int, int]] = set()
        self._next_file_id = 1
        self._next_tag_id = 1

    # ---------- helpers ----------
    def _find_tag(self, name: str) -> Optional[Tag]:
        for tag in self._tags.values():
            if tag.name.casefold() == name.casefold():
                return tag
        return None

    def _ensure_tag(self, name: str, colour: Optional[str] = None) -> int:
        tag = self._find_tag(name)
        if tag:
            return tag.id
        tag = Tag(id=self._next_tag_id, name=name, colour=colour)
        self._tags[tag.id] = tag
        self._next_tag_id += 1
        return tag.id

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

    def delete_file(self, file_id: int) -> bool:
        if file_id not in self._files:
            return False
        del self._files[file_id]
        self._links = {(f, t) for (f, t) in self._links if f != file_id}
        return True

    # ---------- tags ----------
    def _is_ancestor(self, candidate: int, tag_id: int) -> bool:
        seen: Set[int] = set()
        cur: Optional[int] = tag_id
        while cur is not None and cur not in seen:
            if cur == candidate:
                return True
            seen.add(cur)
            tag = self._tags.get(cur)
            cur = tag.parent_id if tag else None
        return False

    def create_tag(self, name: str, colour: Optional[str] = None) -> bool:
        if self._find_tag(name):
            return False
        tag = Tag(id=self._next_tag_id, name=name, colour=colour)
        self._tags[tag.id] = tag
        self._next_tag_id += 1
        return True

    def delete_tag(self, name: str) -> bool:
        tag = self._find_tag(name)
        if not tag:
            return False
        for other in self._tags.values():
            if other.parent_id == tag.id:
                other.parent_id = tag.parent_id
        del self._tags[tag.id]
        self._links = {(f, t) for (f, t) in self._links if t != tag.id}
        return True

    def set_tag_parent(self, tag: str, parent: Optional[str]) -> bool:
        node = self._find_tag(tag)
        if not node:
            return False
        if parent is None:
            node.parent_id = None
            return True
        parent_tag = self._find_tag(parent)
        if not parent_tag or self._is_ancestor(parent_tag.id, node.id):
            return False
        node.parent_id = parent_tag.id
        return True

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

    def prune_orphan_tags(self) -> int:
        parents = {t.parent_id for t in self._tags.values() if t.parent_id}
        orphans = [
            tid for tid in self._tags
            if self._usage(tid) == 0 and tid not in parents
        ]
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

    def search_by_name(self, query: str, limit: int = 100) -> List[FileEntry]:
        q = query.casefold()
        hits = [
            self._clone(e)
            for e in self._files.values()
            if q in e.filename.casefold()
        ]
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