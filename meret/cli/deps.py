"""Shared CLI dependencies: where the database lives and how to open it.

Rule: no silent creation. No database means a loud error with a hint.

Only `meret init` creates one (create=True).
"""

import logging
import os
from pathlib import Path
from typing import Optional

import typer
import click
from rich.console import Console

from meret.core.repository import AbstractRepository
from meret.core.sqlite_repo import SQLiteRepository
from meret.core.config import get_config
from meret.core.locale import _

DB_NAME = "meret.db"
ENV_DB_PATH = "MERET_DB"
#: backwards-compatible alias for the old name
DEFAULT_DB_NAME = DB_NAME

# stdout, not stderr: CliRunner.output reads exactly that
_console = Console()

logger = logging.getLogger(__name__)


class DatabaseNotFound(Exception):
    """Database file not found."""

    def __init__(self, path: Path):
        self.path = path
        super().__init__(f"{_('database not found')}: {path}")

    def _render(self) -> None:
        """Render error message - call explicitly when needed."""
        _console.print()
        _console.print(f"[bold red]✗ {_('Database not found')}[/bold red]")
        _console.print(f" {_('looked here:')} [yellow]{self.path}[/yellow]")
        _console.print()
        _console.print(f"[dim]{_('What to do:')}[/dim]")
        _console.print(f" • {_('create a database in the current directory:')} [green]meret init[/green]")
        _console.print(f" • {_('pass the path explicitly:')} [green]meret scan --db /path/to/meret.db ...[/green]")
        _console.print(f" • {_('or set the environment variable:')} [green]MERET_DB=/path/to/meret.db[/green]")
        _console.print()


def _db_file(given: Path | str) -> Path:
    """A directory means <dir>/meret.db; anything else is the .db file itself."""
    p = Path(given).expanduser()  # before is_dir(): "~/Media" is a directory too
    return (p / DB_NAME if p.is_dir() else p).resolve()


def resolve_db_path(path: Optional[Path] = None) -> Path:
    """
    Resolve database path from CLI arg, environment or config.

    Priority:
    1. Explicit path argument (--db, or init's PATH)
    2. $MERET_DB
    3. Config paths.default_db
    4. Current directory / meret.db
    """
    if path is not None:
        resolved = _db_file(path)
        logger.debug("db path from argument %s -> %s", path, resolved)
        return resolved

    # An env var is a deliberate choice, so it beats the config
    env_path = os.environ.get(ENV_DB_PATH)
    if env_path:
        resolved = _db_file(env_path)
        logger.debug("db path from $%s -> %s", ENV_DB_PATH, resolved)
        return resolved

    # Try config
    config = get_config()
    db_path = config.get("paths.default_db")
    if db_path:
        resolved = Path(db_path).expanduser().resolve()
        logger.debug("db path from config paths.default_db -> %s", resolved)
        return resolved

    # Default: current dir
    resolved = Path.cwd() / "meret.db"
    logger.debug("db path defaulted to the current directory -> %s", resolved)
    return resolved


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


def get_repo(db_path: Path | str | None = None, create: bool = False) -> SQLiteRepository:
    resolved = resolve_db_path(db_path)
    if not resolved.exists() and not create:
        logger.warning("database not found: %s", resolved)
        exc = DatabaseNotFound(resolved)
        exc._render()
        raise exc
    return SQLiteRepository(str(resolved))