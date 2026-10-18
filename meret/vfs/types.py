# src/meret/vfs/types.py

from dataclasses import dataclass
from pathlib import Path


@dataclass
class DirEntry:
    """Virtual directory entry."""
    
    name: str
    vpath: str  # Виртуальный путь (ключ!)
    is_dir: bool
    size: int = 0
    mtime: float = 0.0
    
    @classmethod
    def from_db_row(cls, row: dict, root_vpath: str = "/") -> "DirEntry":
        """Create from database row."""
        name = row.get("name", "unknown")
        is_dir = bool(row.get("is_dir", 0))
        
        # Формируем vpath
        if root_vpath == "/":
            vpath = f"/{name}"
        else:
            vpath = f"{root_vpath}/{name}"
        
        return cls(
            name=name,
            vpath=vpath,
            is_dir=is_dir,
            size=row.get("size", 0),
            mtime=row.get("mtime", 0.0),
        )