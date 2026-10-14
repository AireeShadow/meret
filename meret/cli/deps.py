"""Общие зависимости CLI: где лежит база и как её открыть.

Правило: никакого тихого создания. Нет базы — громкая ошибка с подсказкой.
Создаёт только `meret init` (create=True).
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import click
from rich.console import Console

from meret.core.repository import AbstractRepository
from meret.core.sqlite_repo import SQLiteRepository

DB_NAME = "meret.db"
ENV_DB_PATH = "MERET_DB"

#: совместимость со старым именем
DEFAULT_DB_NAME = DB_NAME

# stdout, а не stderr: CliRunner.output читает именно его
_console = Console()


class DatabaseNotFound(click.ClickException):
    """База не найдена.

    Текст печатается прямо в __init__ (в stdout), а show() — пустой.
    Так сообщение доходит до пользователя и когда click перехватывает
    исключение сам, и когда оно всплывает наружу (как в тестах через
    CliRunner). Двойного вывода при этом не будет.
    """

    def __init__(self, path: Path):
        self.path = path
        super().__init__(f"база данных не найдена: {path}")
        self._render()

    def _render(self) -> None:
        _console.print()
        _console.print("[bold red]✗ База данных не найдена[/bold red]")
        _console.print(f"  искала здесь: [yellow]{self.path}[/yellow]")
        _console.print()
        _console.print("[bold]Что делать:[/bold]")
        _console.print("  • создать базу в текущей директории: [green]meret init[/green]")
        _console.print("  • указать путь явно: [green]meret scan --db /путь/к/meret.db ...[/green]")
        _console.print(
            f"  • или задать переменную окружения: "
            f"[green]{ENV_DB_PATH}=/путь/к/meret.db[/green]"
        )
        _console.print()

    def show(self, file=None) -> None:
        """Уже напечатано в __init__."""
        return


def resolve_db_path(db_path: Optional[Path] = None) -> Path:
    """Приоритет: --db → MERET_DB → ./meret.db.

    Путь без суффикса .db считается каталогом, куда кладём meret.db.
    """
    raw = db_path if db_path is not None else os.environ.get(ENV_DB_PATH)
    if raw is None:
        return (Path.cwd() / DB_NAME).resolve()

    path = Path(raw).expanduser()
    if path.suffix.lower() == ".db":
        return path.resolve()
    return (path / DB_NAME).resolve()


def get_repo(db_path: Optional[Path] = None, create: bool = False) -> AbstractRepository:
    """Репозиторий. create=False: базы нет — DatabaseNotFound."""
    path = resolve_db_path(db_path)

    if path.exists():
        if path.is_dir():
            raise click.ClickException(f"путь к базе — это директория: {path}")
    elif not create:
        raise DatabaseNotFound(path)

    path.parent.mkdir(parents=True, exist_ok=True)
    return SQLiteRepository(str(path))