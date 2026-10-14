from abc import ABC, abstractmethod
from typing import List, Optional

from meret.core.models import FileEntry, Tag, TagNode


class AbstractRepository(ABC):
    """Контракт хранилища. Реализации: SQLite, Postgres, Mock."""

    # --- файлы ---
    @abstractmethod
    def get_file_by_id(self, file_id: int) -> Optional[FileEntry]: ...

    @abstractmethod
    def get_file_by_path(self, path: str) -> Optional[FileEntry]: ...

    @abstractmethod
    def save_file(self, file: FileEntry) -> int: ...

    @abstractmethod
    def delete_file(self, file_id: int) -> bool: ...

    # --- теги ---
    @abstractmethod
    def add_tag(self, file_id: int, tag: str, colour: Optional[str] = None) -> int: ...

    @abstractmethod
    def remove_tag(self, file_id: int, tag: str) -> bool: ...

    @abstractmethod
    def set_file_tags(self, file_id: int, tags: List[str]) -> bool: ...

    @abstractmethod
    def rename_tag(self, old: str, new: str) -> bool: ...

    @abstractmethod
    def set_tag_parent(self, tag: str, parent: Optional[str]) -> bool:
        """Перевесить тег под другого родителя (parent=None → в корень).

        False — если тег или родитель не найдены, либо получился бы цикл
        (тег под своим же потомком или сам под собой).
        """

    @abstractmethod
    def get_all_tags(self) -> List[Tag]: ...

    @abstractmethod
    def get_tag_tree(self) -> List[TagNode]: ...

    @abstractmethod
    def get_tag_usage(self, limit: int = 100) -> List[Tag]: ...

    @abstractmethod
    def prune_orphan_tags(self) -> int: ...

    @abstractmethod
    def create_tag(self, name: str, colour: Optional[str] = None) -> bool: ...

    @abstractmethod
    def delete_tag(self, name: str) -> bool: ...

    # --- поиск ---
    @abstractmethod
    def search_by_tags(
        self,
        tags: List[str],
        match_all: bool = False,
        file_type: Optional[str] = None,
        limit: int = 100,
    ) -> List[FileEntry]: ...

    @abstractmethod
    def search_by_name(self, query: str, limit: int = 100) -> List[FileEntry]: ...

    @abstractmethod
    def get_stats(self) -> dict: ...