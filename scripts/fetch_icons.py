"""Vendor the Lucide glyphs the toolbar uses into meret/gui/icons/commands/.

Lucide is ISC licensed, so its LICENSE is copied in beside the SVGs. Run this by
hand when a command appears or a glyph changes - nothing is fetched at runtime, and
a missing file is not fatal: icons.py falls back to a drawn tile.

    python scripts/fetch_icons.py [--force]
"""

import argparse
import sys
import urllib.error
import urllib.request
from collections.abc import Iterable
from pathlib import Path

#: The default branch, not a tag: the vendored files are committed anyway, so a glyph
#: that moves upstream shows up as a reviewable diff instead of a silent change.
BASE = "https://raw.githubusercontent.com/lucide-icons/lucide/main"
SUFFIX = ".svg"

REPO = Path(__file__).resolve().parent.parent
ICON_DIR = REPO / "meret" / "gui" / "icons" / "commands"
LICENSE = ICON_DIR.parent / "LICENSE"

#: command -> candidate Lucide stems, best first. Lucide renames glyphs between
#: releases - bar-chart-3 became chart-column, trash-2 became trash - so candidates
#: are tried in order. The vendored file is named after the *command*, which is why a
#: rename upstream never reaches the code.
GLYPHS: dict[str, tuple[str, ...]] = {
    "refresh": ("refresh-cw",),
    "scan": ("folder-search",),
    "search": ("search",),
    "tag": ("tag",),
    "untag": ("tag-off", "tag-x", "tags"),
    "delete": ("trash-2", "trash"),
    "sort_files": ("arrow-up-down",),
    "init_db": ("database",),
    "config": ("settings",),
    "stats": ("chart-column", "bar-chart-3", "chart-bar"),
    "tree": ("list-tree",),
    "prune": ("scissors",),
    "mount": ("hard-drive",),
    "umount": ("eject",),
    "command_palette": ("terminal",),
    "help": ("circle-question-mark", "circle-help", "help-circle", "info"),
    "quit": ("log-out",),
}


def download(url: str, target: Path) -> str | None:
    """Save `url` as `target`. None when it worked, otherwise why it did not."""
    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            target.write_bytes(response.read())
    except (OSError, urllib.error.URLError) as error:
        return str(error)
    return None


def fetch_one(command: str, stems: tuple[str, ...], force: bool) -> str | None:
    """Save the first candidate stem that exists. None when the file is already there."""
    target = ICON_DIR / f"{command}{SUFFIX}"
    if target.exists() and not force:
        return None

    last = ""
    for stem in stems:
        problem = download(f"{BASE}/icons/{stem}{SUFFIX}", target)
        if problem is None:
            print(f"  + {target.name} <- {stem}")
            return None
        last = problem
    return f"{command}: none of {', '.join(stems)} ({last})"


def prune(keep: Iterable[str]) -> int:
    """Delete glyphs no command asks for any more - a renamed command leaves them."""
    wanted = set(keep)
    removed = 0
    for path in sorted(ICON_DIR.glob(f"*{SUFFIX}")):
        if path.stem not in wanted:
            path.unlink()
            print(f"  - {path.name}")
            removed += 1
    return removed


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--force", action="store_true", help="re-download existing files")
    args = parser.parse_args(argv)

    ICON_DIR.mkdir(parents=True, exist_ok=True)
    problems = [
        problem
        for problem in (
            fetch_one(command, stems, args.force)
            for command, stems in sorted(GLYPHS.items())
        )
        if problem
    ]

    problem = download(f"{BASE}/LICENSE", LICENSE)
    if problem:
        problems.append(f"LICENSE: {problem}")
    elif args.force:
        print(f"  + {LICENSE.name}")

    print(f"{len(GLYPHS)} glyphs in {ICON_DIR}, {prune(GLYPHS)} removed")
    for problem in problems:
        print(f"! {problem}", file=sys.stderr)
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())