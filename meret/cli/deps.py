"""Shared CLI dependencies: where the database lives and how to open it.

Rule: no silent creation. No database means a loud error with a hint.
Only `meret init` creates one (create=True).
"""

import os
from pathlib import Path
from typing import Optional

import typer

import click
from rich.console import Console

from meret.core.repository import AbstractRepository
from meret.core.sqlite_repo import SQLiteRepository
from meret.core.config import get_config

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


def resolve_db_path(path: Optional[Path] = None) -> Path:
    """
    Resolve database path from CLI arg or config.
    
    Priority:
    1. Explicit path argument
    2. Config paths.default_db
    3. Current directory / .meret.db
    """
    if path is not None:
        # If path is a directory, append default filename
        if path.is_dir():
            return path / "meret.db"
        return path.expanduser().resolve()
    
    # Try config
    config = get_config()
    db_path = config.get("paths.default_db")
    
    if db_path:
        return Path(db_path).expanduser().resolve()
    
    # Default: current dir
    return Path.cwd() / "meret.db"


def resolve_library_path(path: Optional[Path] = None) -> Optional[Path]:
    """
    Resolve library path from CLI arg or config.
    """
    if path is not None:
        return path.expanduser().resolve()
    
    config = get_config()
    lib_path = config.get("paths.default_library")
    
    if lib_path:
        return Path(lib_path).expanduser().resolve()
    
    return None


def get_repo(db_path: Optional[Path] = None, create: bool = False):
    """
    Get repository instance with resolved path.
    """
    resolved = resolve_db_path(db_path)
    
    if not resolved.exists() and not create:
        raise DatabaseNotFound(resolved) 
    
    return SQLiteRepository(str(resolved))
