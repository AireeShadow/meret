import logging
import os
import sys
import typer
from collections import deque
from pathlib import Path
from typing import List, Optional

from rich.console import Console
from rich.table import Table
from rich.tree import Tree
from rich.progress import BarColumn, Progress, TextColumn, TimeElapsedColumn

from meret.core import tagpaths
from meret.core.locale import _, ngettext, set_locale
from meret.core.indexer import PHASE_COLLECT, PHASE_EXTRACT, PHASE_INDEX, Indexer
from meret.core.models import Tag
from meret.core.repository import AbstractRepository
from meret.core.config import get_config, CONFIG_FILENAME
from meret.core.logger import (
    ENV_LEVEL,
    disable_console_logging,
    log_file_path,
    log_uncaught_exceptions,
    setup_logging,
)

from meret import __version__
from meret.cli.deps import get_repo, resolve_db_path

app = typer.Typer(help="Meret — tagged media file system", no_args_is_help=True)
console = Console()
logger = logging.getLogger(__name__)

DB_OPTION = typer.Option(None, "--db", "-d", help="Database directory or path to a .db file")


@app.callback()
def main(
    ctx: typer.Context,
    verbose: bool = typer.Option(False, "--verbose", "-v"),
    log_level: Optional[str] = typer.Option(None, "--log-level"),
    language: Optional[str] = typer.Option(
        None, "--lang", "-l",
        help="Language: en, ru, … (default: config locale.language, else the system's)",
    ),
):
    """Meret — tagged media file system."""
    try:
        path = setup_logging("DEBUG" if verbose else log_level, console=verbose)
    except ValueError as e:
        raise typer.BadParameter(str(e), param_hint="--log-level")
    log_uncaught_exceptions()

    # after logging, so a missing or broken translation is reported in the log
    lang = set_locale(language or get_config().get("locale.language")).language
    logger.info(
        "meret %s: %s | lang=%s argv=%s cwd=%s log=%s",
        __version__, ctx.invoked_subcommand, lang, sys.argv[1:], os.getcwd(), path,
    )


@app.command()
def logs(
    lines: int = typer.Option(0, "--lines", "-n", help="Also print the last N lines"),
):
    """Show where the log file is (and optionally its tail)."""
    path = log_file_path()
    console.print(str(path), soft_wrap=True)
    if lines > 0 and path.exists():
        with path.open(encoding="utf-8", errors="replace") as f:
            tail = deque(f, maxlen=lines)
        console.print("".join(tail), end="", markup=False, highlight=False, soft_wrap=True)


# ----- database -----


@app.command()
def init(
    path: Optional[Path] = typer.Argument(
        None, help="Directory for the database or path to a .db file (default: current dir)"
    ),
    force: bool = typer.Option(False, "--force", "-f", help="Recreate if the database exists"),
):
    """Create the database. The only command that creates it."""
    db_file = resolve_db_path(path)

    if db_file.exists():
        if not force:
            msg = _("Database already exists: {path}").format(path=db_file)
            console.print(f"[yellow]{msg}[/yellow]")
            raise typer.Exit(1)
        for suffix in ("", "-wal", "-shm"):
            Path(f"{db_file}{suffix}").unlink(missing_ok=True)

    get_repo(path, create=True)
    msg = _("Database created: {path}").format(path=db_file)
    console.print(f"[green]✓ {msg}[/green]")


@app.command()
def where(db_path: Optional[Path] = DB_OPTION):
    """Show which database meret sees."""
    path = resolve_db_path(db_path)
    if path.exists():
        mark = f"[green]✓ {_('exists')}[/green]"
    else:
        mark = f"[red]✗ {_('missing')}[/red]"
    console.print(f"{path}  {mark}")


# ----- scan -----


@app.command()
def scan(
    library: Path = typer.Argument(..., help="What to scan"),
    db_path: Optional[Path] = DB_OPTION,
    dry_run: bool = typer.Option(False, "--dry-run", "-n"),
    skip_existing: bool = typer.Option(
        False, "--skip-existing", "-s",
        help="Skip files that are already indexed and unchanged on disk",
    ),
    workers: int = typer.Option(8, "--workers", "-w", help="Threads for reading metadata"),
):
    """Scan and index media."""
    if not library.exists():
        console.print(f"[red]{_('No such path: {path}').format(path=library)}[/red]")
        raise typer.Exit(1)

    indexer = Indexer(get_repo(db_path), library, workers=workers, skip_existing=skip_existing)

    with Progress(
        TextColumn("[cyan]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        tasks = {
            PHASE_COLLECT: progress.add_task(_("Finding files"), total=None),
            PHASE_EXTRACT: progress.add_task(_("Reading metadata"), total=None),
            PHASE_INDEX: progress.add_task(_("Writing to database"), total=None),
        }

        def on_progress(phase: str, done: int, total: int) -> None:
            progress.update(tasks[phase], total=total, completed=done)

        stats = indexer.run(dry_run=dry_run, progress_callback=on_progress)

    if stats.get("interrupted"):
        console.print(f"[yellow]⚠ {_('Interrupted by user')}[/yellow]")

    if dry_run:
        msg = _("New files: {found}, skipped: {skipped}").format(
            found=stats["found"], skipped=stats["skipped"]
        )
        console.print(f"[cyan]{msg}[/cyan]")
    else:
        msg = _("New: {new}, updated: {updated}, skipped: {skipped}, errors: {errors}").format(
            new=stats["indexed"],
            updated=stats["updated"],
            skipped=stats["skipped"],
            errors=stats["errors"],
        )
        console.print(f"[green]✓ {msg}[/green]")


# ----- search -----


@app.command()
def search(
    query: str = typer.Argument("", help="Text in the file name"),
    db_path: Optional[Path] = DB_OPTION,
    tags: Optional[List[str]] = typer.Option(None, "--tag", "-g", help="Tag path: genre/mystery"),
    match_all: bool = typer.Option(False, "--all", help="Files with ALL the tags"),
    file_type: Optional[str] = typer.Option(None, "--type", "-t"),
):
    """Search by name and/or tags. A tag path also matches its whole subtree."""
    repo = get_repo(db_path)

    if tags:
        all_tags = repo.get_all_tags()
        groups = [tagpaths.subtree_ids(path, all_tags) for path in tags]
        results = repo.search_by_tag_ids(groups, match_all=match_all, file_type=file_type)
    else:
        results = repo.search_by_name(query) if query else []

    if not results:
        console.print(f"[yellow]{_('Nothing found')}[/yellow]")
        return

    table = Table(title=_("Found: {n}").format(n=len(results)))
    for col, style in (
        (_("File"), "cyan"),
        (_("Type"), "green"),  # values stay as typed in --type: image, video, audio
        (_("Rating"), "yellow"),
        (_("Tags"), "magenta"),
    ):
        table.add_column(col, style=style)

    for f in results[:50]:
        labels = ", ".join(_path_of(repo, n) for n in f.labels)
        table.add_row(f.filename, f.type.value, "⭐" * f.rating, labels)
    console.print(table)


# ----- tags -----


def _print_tree(repo: AbstractRepository) -> None:
    def build(nodes, node):
        for n in sorted(nodes, key=lambda x: x.name.casefold()):
            child = node.add(f"{n.name} [dim]({n.usage_count})[/dim]")
            build(n.children, child)

    root = Tree(f"[bold]{_('Tags')}[/bold]")
    build(repo.get_tag_tree(), root)
    console.print(root)


def _ensure_path(
    repo: AbstractRepository, path: str, colour: Optional[str] = None
) -> Optional[Tag]:
    """Tag by path; missing parents are created. None for an empty path."""
    path = tagpaths.normalize(path)
    if not path:
        return None
    repo.create_tag(path, colour)  # the repository builds the parents itself
    return tagpaths.resolve(path, repo.get_all_tags())


def _path_of_tag(repo: AbstractRepository, tag: Tag) -> str:
    """Full tag path — the repository resolves it unambiguously."""
    return tagpaths.path_of(tag, {t.id: t for t in repo.get_all_tags()})


def _path_of(repo: AbstractRepository, name: str) -> str:
    """Path by tag name — for showing a file's tags. With namesakes, the shortest wins."""
    return tagpaths.name_to_path(repo.get_all_tags()).get(name.casefold(), name)


@app.command()
def tag(
    filepath: Path = typer.Argument(..., help="File"),
    labels: List[str] = typer.Argument(..., help="Tag paths: genre/mystery"),
    db_path: Optional[Path] = DB_OPTION,
    remove: bool = typer.Option(False, "--remove", "-r"),
):
    """Add/remove tags on a file. A path creates any missing parents."""
    repo = get_repo(db_path)
    entry = repo.get_file_by_path(str(filepath))
    if not entry:
        msg = _("File is not in the database: {path}").format(path=filepath)
        console.print(f"[red]{msg}[/red]")
        raise typer.Exit(1)

    done: List[str] = []
    for label in labels:
        found = (
            tagpaths.resolve(label, repo.get_all_tags()) if remove else _ensure_path(repo, label)
        )
        if found is None:
            console.print(f"[yellow]{_('No such path: {path}').format(path=label)}[/yellow]")
            continue
        full = _path_of_tag(repo, found)
        if remove:
            repo.remove_tag(entry.id, full)
        else:
            repo.add_tag(entry.id, full)
        done.append(full)

    updated = repo.get_file_by_id(entry.id)
    msg = (_("Removed: {tags}") if remove else _("Added: {tags}")).format(tags=", ".join(done))
    console.print(f"[green]✓ {msg}[/green]")
    current = ", ".join(_path_of(repo, n) for n in updated.labels) or "—"
    console.print(f"[dim]{_('Tags: {tags}').format(tags=current)}[/dim]")


@app.command("tag-new")
def tag_new(
    path: str = typer.Argument(..., help="Tag path: genre/mystery"),
    colour: Optional[str] = typer.Option(None, "--colour", "-c"),
    db_path: Optional[Path] = DB_OPTION,
):
    """Create a tag and all of its missing parents along the path."""
    repo = get_repo(db_path)
    found = _ensure_path(repo, path, colour)
    if found is None:
        console.print(f"[red]{_('Empty path')}[/red]")
        raise typer.Exit(1)
    msg = _("Tag: {path}").format(path=_path_of_tag(repo, found))
    console.print(f"[green]✓ {msg}[/green]")


@app.command("tag-del")
def tag_del(
    path: str = typer.Argument(..., help="Tag path"),
    db_path: Optional[Path] = DB_OPTION,
):
    """Delete a tag. Its children move up to its parent's level."""
    repo = get_repo(db_path)
    found = tagpaths.resolve(path, repo.get_all_tags())
    if found is None:
        console.print(f"[red]{_('No such path: {path}').format(path=path)}[/red]")
        raise typer.Exit(1)
    repo.delete_tag(_path_of_tag(repo, found))
    console.print(f"[green]✓ {_('Deleted: {path}').format(path=path)}[/green]")


@app.command()
def parent(
    child: str = typer.Argument(..., help="Path of the tag to move"),
    new_parent: Optional[str] = typer.Argument(None, help="New parent. Omit to move to the root"),
    db_path: Optional[Path] = DB_OPTION,
):
    """Move a tag (by path) under a new parent. Hierarchy = folders in the virtual FS."""
    repo = get_repo(db_path)
    tags = repo.get_all_tags()

    node = tagpaths.resolve(child, tags)
    if node is None:
        console.print(f"[red]{_('No such path: {path}').format(path=child)}[/red]")
        raise typer.Exit(1)

    target = None
    if new_parent is not None:
        target = tagpaths.resolve(new_parent, tags)
        if target is None:
            console.print(f"[red]{_('No such path: {path}').format(path=new_parent)}[/red]")
            raise typer.Exit(1)

    by_id = {t.id: t for t in tags}

    # clear diagnostics: walk up from the new parent looking for our tag
    walk = target
    while walk is not None:
        if walk.id == node.id:
            msg = _("{parent} is inside {child} — would create a cycle").format(
                parent=new_parent, child=child
            )
            console.print(f"[red]{msg}[/red]")
            raise typer.Exit(1)
        walk = by_id.get(walk.parent_id)

    node_path = tagpaths.path_of(node, by_id)
    target_path = tagpaths.path_of(target, by_id) if target else None

    if not repo.set_tag_parent(node_path, target_path):
        console.print(f"[red]{_('Failed — possibly it would create a cycle')}[/red]")
        raise typer.Exit(1)

    console.print(f"[green]✓ {node_path} → {target_path or _('root')}[/green]")
    _print_tree(repo)


@app.command("tags")
def list_tags(db_path: Optional[Path] = DB_OPTION):
    """Show tags as paths, with file counts."""
    repo = get_repo(db_path)
    usage = repo.get_tag_usage()
    if not usage:
        console.print(f"[yellow]{_('No tags yet')}[/yellow]")
        return
    paths = tagpaths.name_to_path(repo.get_all_tags())
    table = Table(title=_("Tags: {count}").format(count=len(usage)))
    table.add_column(_("Path"), style="magenta")
    table.add_column(_("Files"), style="cyan", justify="right")
    for t in usage:
        table.add_row(paths.get(t.name.casefold(), t.name), str(t.usage_count))
    console.print(table)


@app.command()
def tree(db_path: Optional[Path] = DB_OPTION):
    """Show the tag hierarchy as a tree."""
    _print_tree(get_repo(db_path))


@app.command("tag-prune")
def tag_prune(db_path: Optional[Path] = DB_OPTION):
    """Delete tags that have no files and no children."""
    n = get_repo(db_path).prune_orphan_tags()
    console.print(f"[green]✓ {_('Tags deleted: {n}').format(n=n)}[/green]")


# ----- misc -----


@app.command()
def stats(db_path: Optional[Path] = DB_OPTION):
    """Database statistics."""
    s = get_repo(db_path).get_stats()
    console.print(_("Files: {files}, tags: {tags}").format(files=s["total_files"], tags=s["total_tags"]))
    for t, c in s["by_type"].items():
        console.print(f"  {t}: {c}")

# ----- config -----
config_app = typer.Typer(help="Configuration management", no_args_is_help=True)
app.add_typer(config_app, name="config")


@config_app.command("show")
def config_show():
    """Display current configuration."""
    from rich.syntax import Syntax
    
    cfg = get_config()
    console.print(f"[cyan]{_('Config file')}:[/cyan] {cfg.config_path}")
    
    # Get effective config as plain dict (not Pydantic models!)
    def to_plain(value):
        """Convert Pydantic models and other objects to plain Python types."""
        if value is None:
            return ""  # TOML doesn't have null, use empty string
        if hasattr(value, "model_dump"):
            return {k: to_plain(v) for k, v in value.model_dump().items()}
        if hasattr(value, "dict"):  # older Pydantic
            return {k: to_plain(v) for k, v in value.dict().items()}
        if isinstance(value, dict):
            return {k: to_plain(v) for k, v in value.items()}
        if isinstance(value, list):
            return [to_plain(v) for v in value]
        return value
    
    # Get raw values and convert
    raw_scan = cfg.get("scan")
    raw_gui = cfg.get("gui")
    raw_locale = cfg.get("locale")
    
    config_dict = {
        "paths": to_plain({
            "default_db": cfg.get("paths.default_db"),
            "default_library": cfg.get("paths.default_library"),
        }),
        "scan": to_plain(raw_scan) if raw_scan else {"workers": 8, "chunk_size": 1000, "skip_existing": True},
        "gui": to_plain(raw_gui) if raw_gui else {"theme": "system", "thumbnail_size": 256, "window_width": 1280, "window_height": 720},
        "locale": to_plain(raw_locale) if raw_locale else {"language": "auto", "date_format": "%Y-%m-%d"},
    }
    
    # Try to render as TOML
    try:
        import tomli_w
        content = tomli_w.dumps(config_dict)
    except ImportError:
        # Fallback: manual TOML-like formatting
        lines = []
        for section, values in config_dict.items():
            lines.append(f"[{section}]")
            for key, val in values.items():
                if val == "" or val is None:
                    lines.append(f'{key} = ""')
                elif isinstance(val, bool):
                    lines.append(f"{key} = {str(val).lower()}")
                elif isinstance(val, (int, float)):
                    lines.append(f"{key} = {val}")
                elif isinstance(val, str):
                    # Escape special chars in strings
                    escaped = val.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n')
                    lines.append(f'{key} = "{escaped}"')
                else:
                    lines.append(f'{key} = "{val}"')
            lines.append("")
        content = "\n".join(lines)
    
    if not cfg.config_path.exists():
        console.print(f"[yellow]{_('(file does not exist — showing defaults)')}[/yellow]")
    
    console.print()
    syntax = Syntax(content, "toml", theme="monokai", line_numbers=True)
    console.print(syntax)


@config_app.command("get")
def config_get(key: str = typer.Argument(..., help="Config key (e.g., scan.workers)")):
    """Get a configuration value."""
    cfg = get_config()
    value = cfg.get(key)
    
    if value is None:
        console.print(f"[yellow]{_('Key {key!r} not found or is empty').format(key=key)}[/yellow]")
        raise typer.Exit(1)
    
    console.print(f"[cyan]{key}[/cyan] = [green]{value}[/green]")


@config_app.command("set")
def config_set(
    key: str = typer.Argument(..., help="Config key (e.g., scan.workers)"),
    value: str = typer.Argument(..., help="Value to set"),
):
    """Set a configuration value."""
    cfg = get_config()
    
    try:
        cfg.set(key, value)
        console.print(f"[green]✓ {_('Set {key} = {value}').format(key=key, value=value)}[/green]")
    except Exception as e:
        logger.warning("config set %s=%r failed", key, value, exc_info=True)
        console.print(f"[red]{_('Failed to set {key}: {error}').format(key=key, error=e)}[/red]")
        raise typer.Exit(1)


@config_app.command("edit")
def config_edit():
    """Open config file in default editor."""
    from meret.core.config import get_config, CONFIG_FILENAME
    
    cfg = get_config()
    config_path = cfg.config_path
    
    # Ensure directory exists
    config_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Create file with defaults if it doesn't exist or is empty
    if not config_path.exists() or config_path.stat().st_size < 10:
        default_content = '''[paths]
default_db = ""
default_library = ""

[scan]
workers = 8
chunk_size = 1000
skip_existing = true

[gui]
theme = "system"
thumbnail_size = 256
window_width = 1280
window_height = 720

[locale]
language = "auto"
date_format = "%Y-%m-%d"
'''
        config_path.write_text(default_content, encoding="utf-8")
        console.print(f"[yellow]{_('Created default config')}[/yellow]")
    
    # Open in editor
    try:
        import os
        import platform
        import subprocess
        
        system = platform.system()
        if system == "Windows":
            os.startfile(str(config_path))
        elif system == "Darwin":  # macOS
            subprocess.run(["open", str(config_path)], check=True)
        else:  # Linux
            subprocess.run(["xdg-open", str(config_path)], check=True)
            
        console.print(f"[green]✓ {_('Opened {path}').format(path=config_path)}[/green]")
    except Exception as e:
        console.print(f"[red]{_('Failed to open editor: {error}').format(error=e)}[/red]")
        console.print(f"[dim]{_('Path: {path}').format(path=config_path)}[/dim]")

@config_app.command("reset")
def config_reset(
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation")
):
    """Reset configuration to defaults."""
    if not force:
        confirm = typer.confirm(_("Reset all settings to defaults?"))
        if not confirm:
            raise typer.Abort()
    
    cfg = get_config()
    cfg.reset()
    console.print(f"[green]✓ {_('Configuration reset to defaults')}[/green]")


@config_app.command("path")
def config_path():
    """Show configuration file path."""
    cfg = get_config()
    if cfg.config_path.exists():
        exists = f"[green]({_('exists')})[/green]"
    else:
        exists = f"[yellow]({_('will be created')})[/yellow]"
    console.print(f"{cfg.config_path} {exists}")

@app.command("tui")
def tui(
    db_path: Optional[Path] = DB_OPTION,
):
    """Launch interactive TUI."""
    from meret.cli.tui import MeretApp

    try:
        app = MeretApp(resolve_db_path(db_path))
        disable_console_logging()  # Only after successful init
        app.run()
    except Exception as e:
        logger.exception("TUI failed")
        console.print(
            f"[red]{_('TUI error: {error}').format(error=e)}[/red] ({_('details: meret logs')})"
        )
        raise typer.Exit(1)

# ls / open / stat / mount / umount register themselves on `app` (must come last:
# vfs_cmds imports `app` from this module)
from meret.cli import vfs_cmds  # noqa: E402,F401

if __name__ == "__main__":
    app()

