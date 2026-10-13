"""Общие зависимости CLI."""
from pathlib import Path

from meret.core.repository import AbstractRepository
from meret.core.sqlite_repo import SQLiteRepository


def get_repo(db_path: Path) -> AbstractRepository:
    """Единственное место создания репозитория."""
    return SQLiteRepository(str(db_path / "meret.db"))