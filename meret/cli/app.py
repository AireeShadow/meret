import typer
from pathlib import Path
from typing import List, Optional

from rich.console import Console
from rich.table import Table
from rich.tree import Tree
from rich.progress import BarColumn, Progress, TextColumn, TimeElapsedColumn

from meret.core import tagpaths
from meret.core.indexer import PHASE_COLLECT, PHASE_EXTRACT, PHASE_INDEX, Indexer
from meret.core.repository import AbstractRepository

from meret.cli.deps import get_repo, resolve_db_path

app = typer.Typer(help="Meret — tagged media file system", no_args_is_help=True)
console = Console()


@app.command()
def init(
    path: Optional[Path] = typer.Argument(None, help="Каталог для базы или путь к .db"),
    force: bool = typer.Option(False, "--force", "-f", help="Пересоздать, если есть"),
):
    """Создать базу."""
    db_file = resolve_db_path(path)

    if db_file.exists():
        if not force:
            console.print(f"[yellow]БД уже есть: {db_file}[/yellow]")
            raise typer.Exit(1)
        db_file.unlink()

    get_repo(path, create=True)
    console.print(f"[green]✓ База создана: {db_file}[/green]")

@app.command()
def scan(
    library: Path = typer.Argument(..., help="Что сканировать"),
    db_path: Optional[Path] = typer.Option(None, "--db", "-d", help="Каталог с базой или путь к .db"),
    dry_run: bool = typer.Option(False, "--dry-run", "-n"),
    skip_existing: bool = typer.Option(
        False, "--skip-existing", "-s",
        help="Не трогать файлы, которые уже есть в базе",
    ),
    workers: int = typer.Option(8, "--workers", "-w", help="Потоков на чтение метаданных"),
):
    """Просканировать и проиндексировать медиа."""
    if not library.exists():
        console.print(f"[red]Нет такого пути: {library}[/red]")
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
            PHASE_COLLECT: progress.add_task("Ищу файлы", total=None),
            PHASE_EXTRACT: progress.add_task("Читаю метаданные", total=None),
            PHASE_INDEX: progress.add_task("Пишу в базу", total=None),
        }

        def on_progress(phase: str, done: int, total: int) -> None:
            progress.update(tasks[phase], total=total, completed=done)

        stats = indexer.run(dry_run=dry_run, progress_callback=on_progress)

    if stats.get("interrupted"):
        console.print("[yellow]⚠ Прервано пользователем[/yellow]")

    if dry_run:
        console.print(
            f"[cyan]Новых файлов: {stats['found']}, пропущено: {stats['skipped']}[/cyan]"
        )
    else:
        console.print(
            f"[green]✓ Новых: {stats['indexed']}, "
            f"обновлено: {stats['updated']}, "
            f"пропущено: {stats['skipped']}, "
            f"ошибок: {stats['errors']}[/green]"
        )


@app.command()
def search(
    query: str = typer.Argument("", help="Текст в имени файла"),
    db_path: Path = typer.Option(Path("."), "--db", "-d"),
    tags: Optional[List[str]] = typer.Option(None, "--tag", "-g", help="Путь тега: жанр/детектив"),
    match_all: bool = typer.Option(False, "--all", help="Файлы со ВСЕМИ тегами"),
    file_type: Optional[str] = typer.Option(None, "--type", "-t"),
):
    """Поиск по имени и/или тегам. Путь тега ищет и по всей его ветке."""
    repo = get_repo(db_path)

    if tags:
        all_tags = repo.get_all_tags()
        names: List[str] = []
        for path in tags:
            names.extend(tagpaths.subtree_names(path, all_tags))
        results = repo.search_by_tags(names, match_all=match_all, file_type=file_type)
    else:
        results = repo.search_by_name(query) if query else []

    if not results:
        console.print("[yellow]Ничего не найдено[/yellow]")
        return

    table = Table(title=f"Найдено: {len(results)}")
    for col, style in (("Файл", "cyan"), ("Тип", "green"), ("Рейтинг", "yellow"), ("Теги", "magenta")):
        table.add_column(col, style=style)

    for f in results[:50]:
        labels = ", ".join(_path_of(repo, n) for n in f.labels)
        table.add_row(f.filename, f.type.value, "⭐" * f.rating, labels)
    console.print(table)


# ----- теги -----

def _print_tree(repo: AbstractRepository) -> None:
    def build(nodes, node):
        for n in sorted(nodes, key=lambda x: x.name.casefold()):
            child = node.add(f"{n.name} [dim]({n.usage_count})[/dim]")
            build(n.children, child)

    root = Tree("[bold]Теги[/bold]")
    build(repo.get_tag_tree(), root)
    console.print(root)


def _ensure_path(repo: AbstractRepository, path: str, colour: Optional[str] = None):
    """Тег по пути; недостающих родителей создаёт. None — пустой путь."""
    path = tagpaths.normalize(path)
    if not path:
        return None

    missing = tagpaths.missing_ancestors(path, repo.get_all_tags())
    for i, missing_path in enumerate(missing):
        leaf = tagpaths.parts(missing_path)[-1]
        if not repo.create_tag(leaf, colour if i == len(missing) - 1 else None):
            continue
        parent = tagpaths.parts(missing_path)[:-1]
        if parent:
            repo.set_tag_parent(leaf, parent[-1])

    return tagpaths.resolve(path, repo.get_all_tags())


def _path_of(repo: AbstractRepository, name: str) -> str:
    return tagpaths.name_to_path(repo.get_all_tags()).get(name.casefold(), name)


@app.command()
def tag(
    filepath: Path = typer.Argument(..., help="Файл"),
    labels: List[str] = typer.Argument(..., help="Пути тегов: жанр/детектив"),
    db_path: Path = typer.Option(Path("."), "--db", "-d"),
    remove: bool = typer.Option(False, "--remove", "-r"),
):
    """Добавить/убрать теги у файла. Путь достраивает недостающих родителей."""
    repo = get_repo(db_path)
    entry = repo.get_file_by_path(str(filepath))
    if not entry:
        console.print(f"[red]Файл не в базе: {filepath}[/red]")
        raise typer.Exit(1)

    done: List[str] = []
    for label in labels:
        found = (
            tagpaths.resolve(label, repo.get_all_tags())
            if remove
            else _ensure_path(repo, label)
        )
        if found is None:
            console.print(f"[yellow]Нет такого пути: {label}[/yellow]")
            continue
        if remove:
            repo.remove_tag(entry.id, found.name)
        else:
            repo.add_tag(entry.id, found.name)
        done.append(_path_of(repo, found.name))

    updated = repo.get_file_by_id(entry.id)
    console.print(f"[green]✓ {'Убрано' if remove else 'Добавлено'}: {', '.join(done)}[/green]")
    console.print(
        f"[dim]Теги: {', '.join(_path_of(repo, n) for n in updated.labels) or '—'}[/dim]"
    )


@app.command("tag-new")
def tag_new(
    path: str = typer.Argument(..., help="Путь тега: жанр/детектив"),
    colour: Optional[str] = typer.Option(None, "--colour", "-c"),
    db_path: Path = typer.Option(Path("."), "--db", "-d"),
):
    """Создать тег и всех недостающих родителей по пути."""
    repo = get_repo(db_path)
    found = _ensure_path(repo, path, colour)
    if found is None:
        console.print("[red]Пустой путь[/red]")
        raise typer.Exit(1)
    console.print(f"[green]✓ Тег: {_path_of(repo, found.name)}[/green]")


@app.command("tag-del")
def tag_del(
    path: str = typer.Argument(..., help="Путь тега"),
    db_path: Path = typer.Option(Path("."), "--db", "-d"),
):
    """Удалить тег. Дети поднимаются на уровень его родителя."""
    repo = get_repo(db_path)
    found = tagpaths.resolve(path, repo.get_all_tags())
    if found is None:
        console.print(f"[red]Нет такого пути: {path}[/red]")
        raise typer.Exit(1)
    repo.delete_tag(found.name)
    console.print(f"[green]✓ Удалён: {path}[/green]")


@app.command()
def parent(
    child: str = typer.Argument(..., help="Путь тега, который перевешиваем"),
    new_parent: Optional[str] = typer.Argument(None, help="Новый родитель. Не указан — в корень"),
    db_path: Path = typer.Option(Path("."), "--db", "-d"),
):
    """Перевесить тег по пути. Иерархия = папки в виртуальной ФС."""
    repo = get_repo(db_path)
    tags = repo.get_all_tags()

    node = tagpaths.resolve(child, tags)
    if node is None:
        console.print(f"[red]Нет такого пути: {child}[/red]")
        raise typer.Exit(1)

    target = None
    if new_parent is not None:
        target = tagpaths.resolve(new_parent, tags)
        if target is None:
            console.print(f"[red]Нет такого пути: {new_parent}[/red]")
            raise typer.Exit(1)

    # понятная диагностика: идём вверх от нового родителя и ищем наш тег
    by_id = {t.id: t for t in tags}
    walk = target
    while walk is not None:
        if walk.id == node.id:
            console.print(f"[red]{new_parent} лежит внутри {child} — вышел бы цикл[/red]")
            raise typer.Exit(1)
        walk = by_id.get(walk.parent_id)

    if not repo.set_tag_parent(node.name, target.name if target else None):
        console.print("[red]Не получилось — возможно, вышел бы цикл[/red]")
        raise typer.Exit(1)

    console.print(f"[green]✓ {child} → {new_parent or 'корень'}[/green]")
    _print_tree(repo)


@app.command("tags")
def list_tags(db_path: Path = typer.Option(Path("."), "--db", "-d")):
    """Показать теги путями, с количеством файлов."""
    repo = get_repo(db_path)
    usage = repo.get_tag_usage()
    if not usage:
        console.print("[yellow]Тегов пока нет[/yellow]")
        return
    paths = tagpaths.name_to_path(repo.get_all_tags())
    table = Table(title=f"Теги: {len(usage)}")
    table.add_column("Путь", style="magenta")
    table.add_column("Файлов", style="cyan", justify="right")
    for t in usage:
        table.add_row(paths.get(t.name.casefold(), t.name), str(t.usage_count))
    console.print(table)


@app.command()
def tree(db_path: Path = typer.Option(Path("."), "--db", "-d")):
    """Показать иерархию тегов как дерево."""
    _print_tree(get_repo(db_path))


@app.command("tag-prune")
def tag_prune(db_path: Path = typer.Option(Path("."), "--db", "-d")):
    """Удалить теги без файлов и без детей."""
    n = get_repo(db_path).prune_orphan_tags()
    console.print(f"[green]✓ Удалено тегов: {n}[/green]")


# ----- прочее -----

@app.command()
def stats(db_path: Path = typer.Option(Path("."), "--db", "-d")):
    """Статистика базы."""
    s = get_repo(db_path).get_stats()
    console.print(f"Файлов: {s['total_files']}, тегов: {s['total_tags']}")
    for t, c in s["by_type"].items():
        console.print(f"  {t}: {c}")

@app.command()
def where(db_path: Optional[Path] = typer.Option(None, "--db", "-d")):
    """Показать, какую базу видит meret."""
    path = resolve_db_path(db_path)
    mark = "[green]✓ есть[/green]" if path.exists() else "[red]✗ нет[/red]"
    console.print(f"{path}  {mark}")


if __name__ == "__main__":
    app()

from meret.cli import vfs_cmds  # noqa: E402,F401  — регистрирует ls/open/stat