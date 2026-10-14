import typer
from pathlib import Path
from typing import List, Optional

from rich.console import Console
from rich.table import Table
from rich.tree import Tree
from rich.progress import BarColumn, Progress, TextColumn, TimeElapsedColumn

from meret.core.indexer import PHASE_COLLECT, PHASE_EXTRACT, Indexer
from meret.core.repository import AbstractRepository
from meret.core.sqlite_repo import SQLiteRepository

from meret.cli.deps import get_repo, resolve_db_path

app = typer.Typer(help="Meret — tagged media file system", no_args_is_help=True)
console = Console()


@app.command()
def init(
    path: Path = typer.Argument(..., help="Каталог или файл БД"),
    force: bool = False,
):
    """Создать базу."""
    db_file = resolve_db_path(path)

    if db_file.exists() and not force:
        console.print(f"[yellow]БД уже есть: {db_file}[/yellow]")
        raise typer.Exit(1)
    if force and db_file.exists():
        db_file.unlink()

    get_repo(path)
    console.print(f"[green]✓ База создана: {db_file}[/green]")


@app.command()
def scan(
    library: Path = typer.Argument(..., help="Что сканировать"),
    db_path: Path = typer.Option(Path("."), "--db", "-d"),
    dry_run: bool = typer.Option(False, "--dry-run", "-n"),
):
    """Просканировать и проиндексировать медиа."""
    if not library.exists():
        console.print(f"[red]Нет такого пути: {library}[/red]")
        raise typer.Exit(1)

    indexer = Indexer(get_repo(db_path), library)

    with Progress(
        TextColumn("[cyan]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        tasks = {
            PHASE_COLLECT: progress.add_task("Ищу файлы", total=None),
            PHASE_EXTRACT: progress.add_task("Читаю метаданные", total=None),
        }

        def on_progress(phase: str, done: int, total: int) -> None:
            progress.update(tasks[phase], total=total, completed=done)

        stats = indexer.run(dry_run=dry_run, progress_callback=on_progress)

    if stats.get("interrupted"):
        console.print("[yellow]⚠ Прервано пользователем[/yellow]")

    if dry_run:
        console.print(f"[cyan]Найдено файлов: {stats['found']}[/cyan]")
    else:
        console.print(
            f"[green]✓ Новых: {stats['indexed']}, "
            f"обновлено: {stats['updated']}, ошибок: {stats['errors']}[/green]"
        )


@app.command()
def search(
    query: str = typer.Argument("", help="Текст в имени файла"),
    db_path: Path = typer.Option(Path("."), "--db", "-d"),
    tags: Optional[List[str]] = typer.Option(None, "--tag", "-g"),
    match_all: bool = typer.Option(False, "--all", help="Файлы со ВСЕМИ тегами"),
    file_type: Optional[str] = typer.Option(None, "--type", "-t"),
):
    """Поиск по имени и/или тегам."""
    repo = get_repo(db_path)

    if tags:
        results = repo.search_by_tags(tags, match_all=match_all, file_type=file_type)
    else:
        results = repo.search_by_name(query) if query else []

    if not results:
        console.print("[yellow]Ничего не найдено[/yellow]")
        return

    table = Table(title=f"Найдено: {len(results)}")
    for col, style in (("Файл", "cyan"), ("Тип", "green"), ("Рейтинг", "yellow"), ("Теги", "magenta")):
        table.add_column(col, style=style)

    for f in results[:50]:
        table.add_row(f.filename, f.type.value, "⭐" * f.rating, ", ".join(f.labels))
    console.print(table)


@app.command()
def tag(
    filepath: Path = typer.Argument(..., help="Файл"),
    labels: List[str] = typer.Argument(..., help="Теги"),
    db_path: Path = typer.Option(Path("."), "--db", "-d"),
    remove: bool = typer.Option(False, "--remove", "-r"),
):
    """Добавить/убрать теги у файла."""
    repo = get_repo(db_path)
    entry = repo.get_file_by_path(str(filepath))
    if not entry:
        console.print(f"[red]Файл не в базе: {filepath}[/red]")
        raise typer.Exit(1)

    for label in labels:
        repo.remove_tag(entry.id, label) if remove else repo.add_tag(entry.id, label)

    updated = repo.get_file_by_id(entry.id)
    console.print(f"[green]✓ {'Убрано' if remove else 'Добавлено'}: {', '.join(labels)}[/green]")
    console.print(f"[dim]Теги: {', '.join(updated.labels) or '—'}[/dim]")


@app.command("tags")
def list_tags(db_path: Path = typer.Option(Path("."), "--db", "-d")):
    """Показать теги с количеством файлов."""
    repo = get_repo(db_path)
    usage = repo.get_tag_usage()
    if not usage:
        console.print("[yellow]Тегов пока нет[/yellow]")
        return
    table = Table(title=f"Теги: {len(usage)}")
    table.add_column("Тег", style="magenta")
    table.add_column("Файлов", style="cyan", justify="right")
    for t in usage:
        table.add_row(t.name, str(t.usage_count))
    console.print(table)


@app.command()
def rename(old: str = typer.Argument(...), new: str = typer.Argument(...),
           db_path: Path = typer.Option(Path("."), "--db", "-d")):
    """Переименовать тег (или слить с существующим)."""
    ok = get_repo(db_path).rename_tag(old, new)
    console.print(f"[green]✓ {old} → {new}[/green]" if ok else f"[red]Нет тега {old}[/red]")


@app.command()
def tree(db_path: Path = typer.Option(Path("."), "--db", "-d")):
    """Показать иерархию тегов как дерево."""
    roots = get_repo(db_path).get_tag_tree()

    def build(nodes, node):
        for n in sorted(nodes, key=lambda x: x.name.lower()):
            child = node.add(f"{n.name} [dim]({n.usage_count})[/dim]")
            build(n.children, child)

    root = Tree("[bold]Теги[/bold]")
    build(roots, root)
    console.print(root)


@app.command()
def stats(db_path: Path = typer.Option(Path("."), "--db", "-d")):
    """Статистика базы."""
    s = get_repo(db_path).get_stats()
    console.print(f"Файлов: {s['total_files']}, тегов: {s['total_tags']}")
    for t, c in s["by_type"].items():
        console.print(f"  {t}: {c}")


if __name__ == "__main__":
    app()

from meret.cli import vfs_cmds  # noqa: E402,F401  — регистрирует ls/open/stat