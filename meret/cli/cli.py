import typer
from pathlib import Path
from typing import Optional
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn

from meret.core.sqlite_repo import SQLiteRepository
from meret.core.indexer import Indexer

app = typer.Typer(help="Meret - Tagged Media File System")
console = Console()


def get_repo(db_path: Path):
    """Фабрика репозитория"""
    return SQLiteRepository(str(db_path / "meret.db"))


@app.command()
def init(
    path: Path = typer.Argument(..., help="Directory to initialize"),
    force: bool = typer.Option(False, "--force", "-f")
):
    """Initialize meret database"""
    db_file = path / "meret.db"
    
    if db_file.exists() and not force:
        console.print(f"[yellow]Database already exists at {db_file}[/yellow]")
        raise typer.Exit(1)
    
    if force and db_file.exists():
        db_file.unlink()
    
    # Создаём через репозиторий (автоматически создаёт таблицы)
    repo = get_repo(path)
    stats = repo.get_stats()
    
    console.print(f"[green]✓ Database initialized at {db_file}[/green]")
    console.print(f"[dim]Total files: {stats['total_files']}[/dim]")


@app.command()
def scan(
    library: Path = typer.Argument(..., help="Directory to scan"),
    db_path: Path = typer.Option(Path("."), "--db", "-d"),
    dry_run: bool = typer.Option(False, "--dry-run", "-n", help="Don't save to database")
):
    """Scan and index media files"""
    if not library.exists():
        console.print(f"[red]Path does not exist: {library}[/red]")
        raise typer.Exit(1)
    
    repo = get_repo(db_path)
    indexer = Indexer(repo, library)
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console
    ) as progress:
        progress.add_task(description="Scanning files...", total=None)
        entries = indexer.scan()
    
    console.print(f"[cyan]Found {len(entries)} media files[/cyan]")
    
    if dry_run:
        table = Table(title="Files (dry run)")
        table.add_column("File", style="cyan")
        table.add_column("Type", style="green")
        table.add_column("Size", style="yellow")
        
        for entry in entries[:20]:  # Показываем первые 20
            size = entry.metadata.get('size', 0) if entry.metadata else 0
            size_mb = f"{size / 1024 / 1024:.1f} MB" if size else "N/A"
            table.add_row(entry.filename, entry.type.value, size_mb)
        
        if len(entries) > 20:
            table.add_row("...", "...", f"+{len(entries) - 20} more")
        
        console.print(table)
    else:
        with Progress() as progress:
            task = progress.add_task("[cyan]Indexing...", total=len(entries))
            
            # Индексируем пачками
            batch_size = 50
            for i in range(0, len(entries), batch_size):
                batch = entries[i:i + batch_size]
                indexer.index(batch)
                progress.update(task, advance=len(batch))
        
        console.print(f"[green]✓ Indexed {len(entries)} files[/green]")


@app.command()
def search(
    query: str = typer.Argument(..., help="Search query"),
    db_path: Path = typer.Option(Path("."), "--db", "-d"),
    file_type: Optional[str] = typer.Option(None, "--type", "-t"),
    tags: Optional[List[str]] = typer.Option(None, "--tag", "-g")
):
    """Search files"""
    repo = get_repo(db_path)
    
    if tags:
        results = repo.search_by_tags(tags, file_type=file_type)
    else:
        # Поиск по имени (пока простой)
        # Можно добавить полнотекстовый поиск потом
        console.print("[yellow]Use --tag for tag search or provide query[/yellow]")
        raise typer.Exit(1)
    
    if not results:
        console.print("[yellow]No results found[/yellow]")
        return
    
    table = Table(title=f"Found {len(results)} files")
    table.add_column("File", style="cyan")
    table.add_column("Type", style="green")
    table.add_column("Rating", style="yellow")
    table.add_column("Tags", style="magenta")
    
    for file in results[:50]:
        table.add_row(
            file.filename,
            file.type.value,
            "⭐" * file.rating,
            ", ".join(file.labels[:3])
        )
    
    console.print(table)


@app.command()
def tag(
    filepath: Path = typer.Argument(..., help="File to tag"),
    labels: List[str] = typer.Argument(..., help="Tags to add"),
    db_path: Path = typer.Option(Path("."), "--db", "-d"),
    remove: bool = typer.Option(False, "--remove", "-r", help="Remove tags instead of adding")
):
    """Add or remove tags from file"""
    repo = get_repo(db_path)
    
    file_entry = repo.get_file_by_path(str(filepath))
    if not file_entry:
        console.print(f"[red]File not in database: {filepath}[/red]")
        console.print("[dim]Run 'meret scan' first[/dim]")
        raise typer.Exit(1)
    
    if remove:
        for label in labels:
            file_entry.remove_label(label)
        action = "removed from"
    else:
        for label in labels:
            file_entry.add_label(label)
        action = "added to"
    
    repo.update_file_labels(file_entry.id, file_entry.labels)
    console.print(f"[green]✓ Tags {action} {filepath.name}[/green]")
    console.print(f"[dim]Current tags: {', '.join(file_entry.labels)}[/dim]")


@app.command()
def stats(
    db_path: Path = typer.Option(Path("."), "--db", "-d")
):
    """Show database statistics"""
    repo = get_repo(db_path)
    db_stats = repo.get_stats()
    tags = repo.get_all_tags()
    
    console.print(f"\n[bold cyan]Database Statistics[/bold cyan]")
    console.print(f"Total files: {db_stats['total_files']}")
    console.print(f"Unique tags: {len(tags)}")
    
    if db_stats['by_type']:
        console.print("\n[bold]By type:[/bold]")
        for ftype, count in db_stats['by_type'].items():
            console.print(f"  {ftype}: {count}")
    
    if tags:
        console.print(f"\n[bold]Tags ({len(tags)}):[/bold]")
        tag_names = [t.name for t in tags[:20]]
        console.print(f"  {', '.join(tag_names)}")
        if len(tags) > 20:
            console.print(f"  ... and {len(tags) - 20} more")


if __name__ == "__main__":
    app()