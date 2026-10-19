"""CLI commands for the virtual FS: ls / open / stat."""

import os
import platform
import subprocess
import signal
from pathlib import Path

from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from meret.cli.app import app
from meret.cli.deps import get_repo, DatabaseNotFound
from meret.vfs import IsADirectory, NotFound, VirtualTree
from meret.core.locale import _

console = Console()


def _human(size: int) -> str:
    """Human-readable size."""
    value = float(size)
    for unit in ("B", "KB", "MB", "GB"):
        if value < 1024 or unit == "GB":
            return f"{value:.0f} {unit}" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} GB"


def _open_external(path: Path) -> None:
    """Opens a file with the system's default application."""
    system = platform.system()
    if system == "Windows":
        os.startfile(path)  # type: ignore[attr-defined]
    elif system == "Darwin":
        subprocess.run(["open", str(path)], check=False)
    else:
        subprocess.run(["xdg-open", str(path)], check=False)


def _get_tree(db_path: Optional[Path]) -> VirtualTree:
    """None = the usual lookup (config paths.default_db, then ./meret.db)."""
    return VirtualTree(get_repo(db_path))


@app.command("ls")
def ls(
    vpath: str = typer.Argument("/", help="Virtual path, e.g. /travel"),
    db_path: Optional[Path] = typer.Option(None, "--db", "-d", help="Database directory or path to a .db file"),
    long: bool = typer.Option(False, "--long", "-l", help="Show sizes"),
):
    """List the contents of a virtual directory (tags = folders)."""
    try:
        tree = _get_tree(db_path)
    except DatabaseNotFound:
        console.print(f"[red]Database not found[/red]")
        raise typer.Exit(1)
    
    try:
        nodes = tree.listdir(vpath)
    except NotFound:
        console.print(f"[red]No such path: {vpath}[/red]")
        raise typer.Exit(1)

    if not nodes:
        console.print("[dim](empty)[/dim]")
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

    console.print(f"\n[dim]{len(dirs)} folders, {len(files)} files[/dim]")


@app.command("open")
def open_file(
    vpath: str = typer.Argument(..., help="Path to the file"),
    db_path: Optional[Path] = typer.Option(None, "--db", "-d", help="Database directory or path to a .db file"),
    path_only: bool = typer.Option(False, "--path", "-p", help="Only print the path"),
):
    """Open a file with the system's default application."""
    tree = _get_tree(db_path)

    try:
        real = tree.real_path(vpath)
    except NotFound:
        console.print(f"[red]File not found: {vpath}[/red]")
        raise typer.Exit(1)
    except IsADirectory:
        console.print(f"[yellow]This is a directory (tag): {vpath}[/yellow]")
        raise typer.Exit(1)

    if not real.exists():
        console.print(f"[yellow]In the database but missing on disk: {real}[/yellow]")
        raise typer.Exit(1)

    if path_only:
        console.print(str(real), soft_wrap=True)  # ← don't wrap
        return

    _open_external(real)
    console.print(f"[green]→ {real}[/green]")


@app.command("stat")
def stat_cmd(
    vpath: str = typer.Argument(..., help="Virtual path"),
    db_path: Optional[Path] = typer.Option(None, "--db", "-d", help="Database directory or path to a .db file"),
):
    """Info about a virtual FS node."""
    tree = _get_tree(db_path)
    try:
        node = tree.stat(vpath)
    except NotFound:
        console.print(f"[red]No such path: {vpath}[/red]")
        raise typer.Exit(1)

    table = Table(show_header=False, box=None)
    table.add_row("path", node.vpath)
    table.add_row("type", "[blue]directory (tag)[/blue]" if node.is_dir else "file")
    if node.size:
        table.add_row("size", _human(node.size))
    if node.source_path:
        table.add_row("on disk", node.source_path)
    console.print(table)


@app.command("mount")
def mount_cmd(
    mountpoint: Path = typer.Argument(..., help="Where to mount, e.g. M:"),
    db_path: Optional[Path] = typer.Option(None, "--db", "-d", help="Database directory or path to a .db file"),
):
    """Mount tags as a drive/directory (read-only)."""
    from meret.vfs.backends.mount import mount as _mount

    console.print(f"[cyan]Mounting at {mountpoint}... Ctrl+C or Enter to exit[/cyan]")
    try:
        _mount(_get_tree(db_path), mountpoint)
    except KeyboardInterrupt:
        console.print("\n[green]Unmounted[/green]")
    except ImportError as e:
        console.print(f"[red]No driver: {e}[/red]")
        console.print("[dim]Linux/macOS: pip install fusepy[/dim]")
        console.print("[dim]Windows: WinFsp + pip install winfspy[/dim]")
        raise typer.Exit(1)


def _pids_for_mountpoint(mountpoint: str) -> list[int]:
    """Finds mount processes by mount point."""
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

    out = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    return [int(tok) for tok in out.stdout.split() if tok.strip().isdigit()]


@app.command("umount")
def umount_cmd(
    mountpoint: str = typer.Argument(..., help="Mount point, e.g. M: or K:\\tags"),
):
    """Unmount: find the mount process and terminate it."""
    pids = [pid for pid in _pids_for_mountpoint(mountpoint) if pid != os.getpid()]

    if not pids:
        console.print(f"[yellow]Nothing is mounted at {mountpoint}[/yellow]")
        raise typer.Exit(1)

    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)  # on Windows this is TerminateProcess
            console.print(f"[green]terminated PID {pid}[/green]")
        except OSError as exc:
            console.print(f"[red]PID {pid}: {exc}[/red]")

    console.print("[dim]If Explorer still shows the mount point, press F5[/dim]")

