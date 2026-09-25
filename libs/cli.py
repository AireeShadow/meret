# libs/cli.py
import typer
from pathlib import Path
from typing import Optional
from rich.console import Console
from rich.table import Table
from rich.progress import Progress

from libs.database import InitializeDb, CommonDbOperations
from libs.indexer import Indexer

app = typer.Typer(help="Meret - Tagged Media File System")
console = Console()


@app.command()
def init(
    path: Path = typer.Argument(..., help="Directory to initialize database"),
    force: bool = typer.Option(False, "--force", "-f", help="Delete existing DB")
):
    """Initialize new Meret database"""
    db_init = InitializeDb(str(path))
    
    if force and Path(db_init.db_location).exists():
        db_init.delete_db()
    
    if db_init.create_tables():
        console.print(f"[green]✓ Database initialized at {db_init.db_location}[/green]")
    else:
        console.print(f"[yellow]Database already exists at {db_init.db_location}[/yellow]")


@app.command()
def index(
    library: Path = typer.Argument(..., help="Directory to scan"),
    db_path: Path = typer.Option(Path("."), "--db", "-d", help="Database location"),
    single: Optional[Path] = typer.Option(None, "--file", "-f", help="Index single file")
):
    """Index media files into database"""
    if not library.exists():
        console.print(f"[red]Error: {library} does not exist[/red]")
        raise typer.Exit(1)
    
    indexer = Indexer(str(library), str(db_path))
    
    with Progress() as progress:
        task = progress.add_task("[cyan]Indexing files...", total=None)
        stats = indexer.run_index(str(single) if single else None)
        progress.update(task, completed=stats['indexed'])
    
    table = Table(title="Indexing Results")
    table.add_column("Metric", style="cyan")
    table.add_column("Count", style="magenta")
    table.add_row("Indexed", str(stats['indexed']))
    table.add_row("Skipped", str(stats['skipped']))
    table.add_row("Errors", str(stats['errors']))
    
    console.print(table)
    indexer.close()


@app.command()
def search(
    query: str = typer.Argument(..., help="Search query"),
    db_path: Path = typer.Option(Path("."), "--db", "-d", help="Database location"),
    type_filter: Optional[str] = typer.Option(None, "--type", "-t", help="Filter by type (image/video/audio)")
):
    """Search files by tags or filename"""
    db = CommonDbOperations(str(db_path))
    
    # Простой поиск по имени (позже добавим поиск по тегам)
    condition = "filename LIKE ?"
    params = (f"%{query}%",)
    
    if type_filter:
        condition += " AND type = ?"
        params += (type_filter,)
    
    results = db.select("Files", condition=condition, params=params, limit=50)
    
    if not results:
        console.print("[yellow]No results found[/yellow]")
        return
    
    table = Table(title=f"Search Results for '{query}'")
    table.add_column("File", style="cyan")
    table.add_column("Type", style="green")
    table.add_column("Rating", style="yellow")
    table.add_column("Tags", style="magenta")
    
    for row in results:
        labels = json.loads(row['labels']) if row['labels'] else []
        table.add_row(
            row['filename'],
            row['type'],
            "⭐" * row['rating'],
            ", ".join(labels[:3])  # Первые 3 тега
        )
    
    console.print(table)
    db.close()


@app.command()
def tag(
    filepath: Path = typer.Argument(..., help="File to tag"),
    tags: list[str] = typer.Argument(..., help="Tags to add"),
    db_path: Path = typer.Option(Path("."), "--db", "-d", help="Database location")
):
    """Add tags to a file"""
    db = CommonDbOperations(str(db_path))
    
    # Получаем текущие теги
    results = db.select("Files", condition="filepath = ?", params=(str(filepath),))
    
    if not results:
        console.print(f"[red]File not found in database: {filepath}[/red]")
        db.close()
        raise typer.Exit(1)
    
    current_labels = json.loads(results[0]['labels']) if results[0]['labels'] else []
    current_labels.extend(tags)
    current_labels = list(set(current_labels))  # Убираем дубликаты
    
    db.update(
        "Files",
        {"labels": current_labels},
        "filepath = ?",
        (str(filepath),)
    )
    
    console.print(f"[green]✓ Tagged {filepath} with: {', '.join(tags)}[/green]")
    db.close()


if __name__ == "__main__":
    app()