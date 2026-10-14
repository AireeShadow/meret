"""CLI-команды виртуальной ФС: ls / open / stat."""
import os
import platform
import subprocess
import signal
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from meret.cli.app import app
from meret.cli.deps import get_repo
from meret.vfs import IsADirectory, NotFound, VirtualTree

console = Console()


def _human(size: int) -> str:
    """Человекочитаемый размер."""
    value = float(size)
    for unit in ("B", "KB", "MB", "GB"):
        if value < 1024 or unit == "GB":
            return f"{value:.0f} {unit}" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} GB"


def _open_external(path: Path) -> None:
    """Открывает файл системным приложением."""
    system = platform.system()
    if system == "Windows":
        os.startfile(path)  # type: ignore[attr-defined]
    elif system == "Darwin":
        subprocess.run(["open", str(path)], check=False)
    else:
        subprocess.run(["xdg-open", str(path)], check=False)


def _get_tree(db_path: Path) -> VirtualTree:
    return VirtualTree(get_repo(db_path))


@app.command("ls")
def ls(
    vpath: str = typer.Argument("/", help="Виртуальный путь, напр. /путешествия"),
    db_path: Path = typer.Option(Path("."), "--db", "-d", help="Каталог с meret.db"),
    long: bool = typer.Option(False, "--long", "-l", help="Показать размеры"),
):
    """Показать содержимое виртуального каталога (теги = папки)."""
    tree = _get_tree(db_path)
    try:
        nodes = tree.listdir(vpath)
    except NotFound:
        console.print(f"[red]Нет такого пути: {vpath}[/red]")
        raise typer.Exit(1)

    if not nodes:
        console.print("[dim]пусто[/dim]")
        return

    dirs = [n for n in nodes if n.is_dir]
    files = [n for n in nodes if not n.is_dir]

    for node in dirs:
        console.print(f"[bold blue]{node.name}/[/bold blue]")
    for node in files:
        if long:
            console.print(f"[dim]{_human(node.size):>9}[/dim]  {node.name}")
        else:
            console.print(node.name)

    console.print(f"\n[dim]{len(dirs)} папок, {len(files)} файлов[/dim]")


@app.command("open")
def open_file(
    vpath: str = typer.Argument(..., help="Путь к файлу"),
    db_path: Path = typer.Option(Path("."), "--db", "-d"),
    path_only: bool = typer.Option(False, "--path", "-p", help="Только вывести путь"),
):
    """Открыть файл системным приложением."""
    tree = _get_tree(db_path)

    try:
        real = tree.real_path(vpath)
    except NotFound:
        console.print(f"[red]Файл не найден: {vpath}[/red]")
        raise typer.Exit(1)
    except IsADirectory:
        console.print(f"[yellow]Это каталог (тег): {vpath}[/yellow]")
        raise typer.Exit(1)

    if not real.exists():
        console.print(f"[yellow]В базе есть, а на диске нет: {real}[/yellow]")
        raise typer.Exit(1)

    if path_only:
        console.print(str(real), soft_wrap=True)   # ← не переносить
        return

    _open_external(real)
    console.print(f"[green]→ {real}[/green]")


@app.command("stat")
def stat_cmd(
    vpath: str = typer.Argument(..., help="Виртуальный путь"),
    db_path: Path = typer.Option(Path("."), "--db", "-d"),
):
    """Информация об узле виртуальной ФС."""
    tree = _get_tree(db_path)
    try:
        node = tree.stat(vpath)
    except NotFound:
        console.print(f"[red]Нет такого пути: {vpath}[/red]")
        raise typer.Exit(1)

    table = Table(show_header=False, box=None)
    table.add_row("путь", node.vpath)
    table.add_row("тип", "[blue]каталог (тег)[/blue]" if node.is_dir else "файл")
    if node.size:
        table.add_row("размер", _human(node.size))
    if node.source_path:
        table.add_row("на диске", node.source_path)
    console.print(table)

@app.command("mount")
def mount_cmd(
    mountpoint: Path = typer.Argument(..., help="Куда монтировать, напр. M:"),
    db_path: Path = typer.Option(Path("."), "--db", "-d"),
):
    """Смонтировать теги как диск/каталог (только чтение)."""
    from meret.vfs.backends.mount import mount as _mount

    console.print(f"[cyan]Монтирую в {mountpoint}... Ctrl+C или Enter для выхода[/cyan]")
    try:
        _mount(_get_tree(db_path), mountpoint)
    except KeyboardInterrupt:
        console.print("\n[green]Отмонтировано[/green]")
    except ImportError as e:
        console.print(f"[red]Нет драйвера: {e}[/red]")
        console.print("[dim]Linux/macOS: pip install fusepy[/dim]")
        console.print("[dim]Windows: WinFsp + pip install winfspy[/dim]")
        raise typer.Exit(1)

def _pids_for_mountpoint(mountpoint: str) -> list[int]:
    """Находит процессы монтирования по точке монтирования."""
    needle = mountpoint.rstrip("\\/")
    if platform.system() == "Windows":
        script = (
            "Get-CimInstance Win32_Process | "
            "Where-Object { $_.CommandLine -like '*meret*mount*' "
            f"-and $_.CommandLine -like '*{needle}*' }} | "
            "Select-Object -ExpandProperty ProcessId"
        )
        cmd = ["powershell", "-NoProfile", "-Command", script]
    else:
        cmd = ["pgrep", "-f", f"meret mount.*{needle}"]

    out = subprocess.run(
        cmd, capture_output=True, text=True, encoding="utf-8", errors="replace"
    )
    return [int(tok) for tok in out.stdout.split() if tok.strip().isdigit()]


@app.command("umount")
def umount_cmd(
    mountpoint: str = typer.Argument(..., help="Точка монтирования, напр. M: или K:\\tags"),
):
    """Отмонтировать: найти процесс монтирования и завершить его."""
    pids = [pid for pid in _pids_for_mountpoint(mountpoint) if pid != os.getpid()]

    if not pids:
        console.print(f"[yellow]Ничего не смонтировано в {mountpoint}[/yellow]")
        raise typer.Exit(1)

    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)      # на Windows это TerminateProcess
            console.print(f"[green]завершил PID {pid}[/green]")
        except OSError as exc:
            console.print(f"[red]PID {pid}: {exc}[/red]")

    console.print("[dim]Если Проводник показывает точку — нажми F5[/dim]")