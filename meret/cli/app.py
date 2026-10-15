import typer
from pathlib import Path
from typing import List, Optional

from rich.console import Console
from rich.table import Table
from rich.tree import Tree
from rich.progress import BarColumn, Progress, TextColumn, TimeElapsedColumn

from meret.core import tagpaths
from meret.core.indexer import PHASE_COLLECT, PHASE_EXTRACT, PHASE_INDEX, Indexer
from meret.core.models import Tag
from meret.core.repository import AbstractRepository

from meret.cli.deps import get_repo, resolve_db_path

app = typer.Typer(help="Meret — tagged media file system", no_args_is_help=True)
console = Console()

DB_OPTION = typer.Option(None, "--db", "-d", help="Database directory or path to a .db file")


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
            console.print(f"[yellow]Database already exists: {db_file}[/yellow]")
            raise typer.Exit(1)
        for suffix in ("", "-wal", "-shm"):
            Path(f"{db_file}{suffix}").unlink(missing_ok=True)

    get_repo(path, create=True)
    console.print(f"[green]✓ Database created: {db_file}[/green]")


@app.command()
def where(db_path: Optional[Path] = DB_OPTION):
    """Show which database meret sees."""
    path = resolve_db_path(db_path)
    mark = "[green]✓ exists[/green]" if path.exists() else "[red]✗ missing[/red]"
    console.print(f"{path}  {mark}")


# ----- scan -----


@app.command()
def scan(
    library: Path = typer.Argument(..., help="What to scan"),
    db_path: Optional[Path] = DB_OPTION,
    dry_run: bool = typer.Option(False, "--dry-run", "-n"),
    skip_existing: bool = typer.Option(
        False, "--skip-existing", "-s", help="Skip files that are already in the database"
    ),
    workers: int = typer.Option(8, "--workers", "-w", help="Threads for reading metadata"),
):
    """Scan and index media."""
    if not library.exists():
        console.print(f"[red]No such path: {library}[/red]")
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
            PHASE_COLLECT: progress.add_task("Finding files", total=None),
            PHASE_EXTRACT: progress.add_task("Reading metadata", total=None),
            PHASE_INDEX: progress.add_task("Writing to database", total=None),
        }

        def on_progress(phase: str, done: int, total: int) -> None:
            progress.update(tasks[phase], total=total, completed=done)

        stats = indexer.run(dry_run=dry_run, progress_callback=on_progress)

    if stats.get("interrupted"):
        console.print("[yellow]⚠ Interrupted by user[/yellow]")

    if dry_run:
        console.print(f"[cyan]New files: {stats['found']}, skipped: {stats['skipped']}[/cyan]")
    else:
        console.print(
            f"[green]✓ New: {stats['indexed']}, "
            f"updated: {stats['updated']}, "
            f"skipped: {stats['skipped']}, "
            f"errors: {stats['errors']}[/green]"
        )


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
        console.print("[yellow]Nothing found[/yellow]")
        return

    table = Table(title=f"Found: {len(results)}")
    for col, style in (
        ("File", "cyan"),
        ("Type", "green"),
        ("Rating", "yellow"),
        ("Tags", "magenta"),
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

    root = Tree("[bold]Tags[/bold]")
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
        console.print(f"[red]File is not in the database: {filepath}[/red]")
        raise typer.Exit(1)

    done: List[str] = []
    for label in labels:
        found = (
            tagpaths.resolve(label, repo.get_all_tags()) if remove else _ensure_path(repo, label)
        )
        if found is None:
            console.print(f"[yellow]No such path: {label}[/yellow]")
            continue
        full = _path_of_tag(repo, found)
        if remove:
            repo.remove_tag(entry.id, full)
        else:
            repo.add_tag(entry.id, full)
        done.append(full)

    updated = repo.get_file_by_id(entry.id)
    console.print(f"[green]✓ {'Removed' if remove else 'Added'}: {', '.join(done)}[/green]")
    console.print(f"[dim]Tags: {', '.join(_path_of(repo, n) for n in updated.labels) or '—'}[/dim]")


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
        console.print("[red]Empty path[/red]")
        raise typer.Exit(1)
    console.print(f"[green]✓ Tag: {_path_of_tag(repo, found)}[/green]")


@app.command("tag-del")
def tag_del(
    path: str = typer.Argument(..., help="Tag path"),
    db_path: Optional[Path] = DB_OPTION,
):
    """Delete a tag. Its children move up to its parent's level."""
    repo = get_repo(db_path)
    found = tagpaths.resolve(path, repo.get_all_tags())
    if found is None:
        console.print(f"[red]No such path: {path}[/red]")
        raise typer.Exit(1)
    repo.delete_tag(_path_of_tag(repo, found))
    console.print(f"[green]✓ Deleted: {path}[/green]")


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
        console.print(f"[red]No such path: {child}[/red]")
        raise typer.Exit(1)

    target = None
    if new_parent is not None:
        target = tagpaths.resolve(new_parent, tags)
        if target is None:
            console.print(f"[red]No such path: {new_parent}[/red]")
            raise typer.Exit(1)

    by_id = {t.id: t for t in tags}

    # clear diagnostics: walk up from the new parent looking for our tag
    walk = target
    while walk is not None:
        if walk.id == node.id:
            console.print(f"[red]{new_parent} is inside {child} — would create a cycle[/red]")
            raise typer.Exit(1)
        walk = by_id.get(walk.parent_id)

    node_path = tagpaths.path_of(node, by_id)
    target_path = tagpaths.path_of(target, by_id) if target else None

    if not repo.set_tag_parent(node_path, target_path):
        console.print("[red]Failed — possibly it would create a cycle[/red]")
        raise typer.Exit(1)

    console.print(f"[green]✓ {node_path} → {target_path or 'root'}[/green]")
    _print_tree(repo)


@app.command("tags")
def list_tags(db_path: Optional[Path] = DB_OPTION):
    """Show tags as paths, with file counts."""
    repo = get_repo(db_path)
    usage = repo.get_tag_usage()
    if not usage:
        console.print("[yellow]No tags yet[/yellow]")
        return
    paths = tagpaths.name_to_path(repo.get_all_tags())
    table = Table(title=f"Tags: {len(usage)}")
    table.add_column("Path", style="magenta")
    table.add_column("Files", style="cyan", justify="right")
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
    console.print(f"[green]✓ Tags deleted: {n}[/green]")


# ----- misc -----


@app.command()
def stats(db_path: Optional[Path] = DB_OPTION):
    """Database statistics."""
    s = get_repo(db_path).get_stats()
    console.print(f"Files: {s['total_files']}, tags: {s['total_tags']}")
    for t, c in s["by_type"].items():
        console.print(f"  {t}: {c}")


if __name__ == "__main__":
    app()

from meret.cli import vfs_cmds  # noqa: E402,F401  — registers ls/open/stat
