"""Общие зависимости CLI."""
from pathlib import Path

from meret.core.repository import AbstractRepository
from meret.core.sqlite_repo import SQLiteRepository


def resolve_db_path(db_path: Path) -> Path:
    """Каталог или путь к .db → путь к файлу БД. Создаёт каталог при необходимости."""
    db_file = db_path if db_path.suffix == ".db" else db_path / "meret.db"
    db_file.parent.mkdir(parents=True, exist_ok=True)
    return db_file


def get_repo(db_path: Path) -> AbstractRepository:
    """Единственное место создания репозитория."""
    return SQLiteRepository(str(resolve_db_path(db_path)))