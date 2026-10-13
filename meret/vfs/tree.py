import posixpath
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple

from meret.core.models import FileEntry, Tag
from meret.core.repository import AbstractRepository
from meret.vfs.errors import IsADirectory, NotFound   # ← было локально

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
    source_path: Optional[str] = None  # реальный путь на диске


def sanitize(name: str) -> str:
    """Приводит имя тега к безопасному для файловых систем виду."""
    name = unicodedata.normalize("NFC", name).strip()
    for ch in _FORBIDDEN:
        name = name.replace(ch, _SAFE_SUB)
    return name or "_"


class VirtualTree:
    """Отображение иерархии тегов на виртуальные пути. Только чтение."""

    def __init__(self, repo: AbstractRepository) -> None:
        self.repo = repo

    # ---------- разбор путей ----------

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

    # ---------- индекс тегов ----------

    def _tag_index(self) -> Dict[Tuple[Optional[int], str], Tag]:
        """{(parent_id, имя_в_нижнем_регистре): tag} — для быстрого поиска."""
        index: Dict[Tuple[Optional[int], str], Tag] = {}
        for tag in self.repo.get_all_tags():
            index[(tag.parent_id, sanitize(tag.name).casefold())] = tag
        return index

    def _children_of(self, parent_id: Optional[int]) -> List[Tag]:
        return [
            t for t in self.repo.get_all_tags() if t.parent_id == parent_id
        ]

    def _resolve_tag(self, vpath: str) -> Optional[Tag]:
        """Находит тег по пути. None — для корня. NotFound — если нет."""
        parts = self._parts(vpath)
        if not parts:
            return None

        index = self._tag_index()
        parent_id: Optional[int] = None
        tag: Optional[Tag] = None
        for part in parts:
            tag = index.get((parent_id, part.casefold()))
            if tag is None:
                raise NotFound(vpath)
            parent_id = tag.id
        return tag

    # ---------- уникальные имена ----------

    @staticmethod
    def _dedupe(names: List[str]) -> List[str]:
        """Разводит совпадающие имена суффиксом ~1, ~2 (стабильно)."""
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
        """Размер из метаданных, иначе — с диска."""
        size = (entry.metadata or {}).get("size_bytes")
        if size:
            return int(size)
        try:
            return Path(entry.filepath).stat().st_size
        except OSError:
            return 0

    # ---------- листинг ----------

    def listdir(self, vpath: str = "/") -> List[Node]:
        """Возвращает содержимое каталога: подтеги (папки) + файлы."""
        vpath = self.normalize(vpath)
        tag = self._resolve_tag(vpath)

        # подкаталоги = дочерние теги
        child_tags = sorted(self._children_of(tag.id if tag else None),
                            key=lambda t: sanitize(t.name).casefold())

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

        # файлы = помеченные этим тегом
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

    # ---------- stat / чтение ----------

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
        """Путь на диске — чтобы отдать файл внешнему плееру."""
        node = self.stat(vpath)
        if node.is_dir or not node.source_path:
            raise IsADirectory(vpath)
        return Path(node.source_path)

    def walk(self, vpath: str = "/") -> Iterator[Node]:
        """Обход всего дерева."""
        for node in self.listdir(vpath):
            yield node
            if node.is_dir:
                yield from self.walk(node.vpath)

