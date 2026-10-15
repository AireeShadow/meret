from abc import ABC, abstractmethod
from typing import Callable, Dict, List, Set, Optional

from meret.core.models import FileEntry, Tag, TagNode


class AbstractRepository(ABC):
    """Storage contract. Implementations: SQLite, Postgres, Mock."""

    # --- files ---
    @abstractmethod
    def get_file_by_id(self, file_id: int) -> Optional[FileEntry]: ...

    @abstractmethod
    def get_file_by_path(self, path: str) -> Optional[FileEntry]: ...

    @abstractmethod
    def save_file(self, file: FileEntry) -> int: ...

    @abstractmethod
    def save_files(
        self,
        files: List[FileEntry],
        chunk_size: int = 1000,
        on_progress: Optional[Callable[[int, int], None]] = None,
    ) -> Dict[str, int]:
        """Batch write: one transaction per chunk_size files.

        Returns {"indexed": n, "updated": m, "errors": k}.
        on_progress(done, total) is called after each chunk.
        """

    @abstractmethod
    def delete_file(self, file_id: int) -> bool: ...

    # --- tags ---
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
        """Move a tag under another parent (parent=None → to the root).

        False if the tag or the parent isn't found, or if it would create a cycle
        (a tag under its own descendant, or under itself).
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

    # --- search ---
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

    @abstractmethod
    def known_paths(self) -> Set[str]:
        """All paths already known to the database.

        One query for the whole scan, so incremental mode doesn't issue a
        SELECT per file (otherwise it's the same story we had with writes).
        """