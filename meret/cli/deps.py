"""Shared CLI dependencies: where the database lives and how to open it.

Rule: no silent creation. No database means a loud error with a hint.
Only `meret init` creates one (create=True).
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

#: backwards-compatible alias for the old name
DEFAULT_DB_NAME = DB_NAME

# stdout, not stderr: CliRunner.output reads exactly that
_console = Console()


class DatabaseNotFound(click.ClickException):
    """Database not found.

    The text is printed right in __init__ (to stdout), and show() is a no-op.
    That way the message reaches the user both when click catches the
    exception itself and when it propagates outward (as in tests via
    CliRunner). There is no double output either way.
    """

    def __init__(self, path: Path):
        self.path = path
        super().__init__(f"database not found: {path}")
        self._render()

    def _render(self) -> None:
        _console.print()
        _console.print("[bold red]✗ Database not found[/bold red]")
        _console.print(f"  looked here: [yellow]{self.path}[/yellow]")
        _console.print()
        _console.print("[bold]What to do:[/bold]")
        _console.print("  • create a database in the current directory: [green]meret init[/green]")
        _console.print(
            "  • pass the path explicitly: [green]meret scan --db /path/to/meret.db ...[/green]"
        )
        _console.print(
            f"  • or set the environment variable: [green]{ENV_DB_PATH}=/path/to/meret.db[/green]"
        )
        _console.print()

    def show(self, file=None) -> None:
        """Already printed in __init__."""
        return


def resolve_db_path(db_path: Optional[Path] = None) -> Path:
    """Priority: --db → MERET_DB → ./meret.db.

    A path without a .db suffix is treated as a directory to put meret.db in.
    """
    raw = db_path if db_path is not None else os.environ.get(ENV_DB_PATH)
    if raw is None:
        return (Path.cwd() / DB_NAME).resolve()

    path = Path(raw).expanduser()
    if path.suffix.lower() == ".db":
        return path.resolve()
    return (path / DB_NAME).resolve()


def get_repo(db_path: Optional[Path] = None, create: bool = False) -> AbstractRepository:
    """Repository. create=False: no database → DatabaseNotFound."""
    path = resolve_db_path(db_path)

    if path.exists():
        if path.is_dir():
            raise click.ClickException(f"database path is a directory: {path}")
    elif not create:
        raise DatabaseNotFound(path)

    path.parent.mkdir(parents=True, exist_ok=True)
    return SQLiteRepository(str(path))
