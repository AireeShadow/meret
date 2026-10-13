from abc import ABC, abstractmethod
from typing import List, Optional, Set
from meret.core.models import FileEntry, Tag


class AbstractRepository(ABC):
    """Абстрактный класс для хранилища файлов"""
    
    @abstractmethod
    def get_file_by_id(self, file_id: int) -> Optional[FileEntry]: ...
    
    @abstractmethod
    def get_file_by_path(self, path: str) -> Optional[FileEntry]: ...
    
    @abstractmethod
    def save_file(self, file: FileEntry) -> int:
        """Возвращает ID сохранённого файла"""
        ...
    
    @abstractmethod
    def search_by_tags(
        self, 
        tags: List[str], 
        match_all: bool = False,
        file_type: Optional[str] = None
    ) -> List[FileEntry]: ...
    
    @abstractmethod
    def get_all_tags(self) -> List[Tag]: ...
    
    @abstractmethod
    def update_file_labels(self, file_id: int, labels: List[str]) -> bool: ...
    
    @abstractmethod
    def delete_file(self, file_id: int) -> bool: ...
    
    @abstractmethod
    def get_stats(self) -> dict: ...