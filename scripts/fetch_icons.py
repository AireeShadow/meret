"""Vendor the toolbar and file-type icons from Lucide into meret/gui/icons/.

Vendored, not fetched at runtime: the GUI must open without a network, and a pinned
tag keeps the shapes from changing under a release.

    python scripts/fetch_icons.py                 # latest release, prints the tag
    python scripts/fetch_icons.py --tag 0.544.0   # reproducible

Files are named after the command, never after the upstream icon: the loader looks
up `icons/<command>.svg`, so an upstream rename (`home` -> `house`,
`bar-chart-3` -> `chart-column`) cannot break the mapping. A wrong name fails
loudly with the HTTP status instead of writing a broken SVG.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path

RAW = "https://raw.githubusercontent.com/lucide-icons/lucide/{tag}/{path}"
LATEST = "https://api.github.com/repos/lucide-icons/lucide/releases/latest"
TARGET = Path(__file__).resolve().parents[1] / "meret" / "gui" / "icons"
TIMEOUT = 30

#: our file name -> upstream Lucide name. Checked against the repo tree, not guessed.
COMMANDS = {
    "refresh": "refresh-cw",
    "scan": "folder-search",
    "search": "search",
    "tag": "tag",
    "untag": "tag-x",
    "delete": "trash",
    "open_file": "external-link",
    "home": "house",
    "up": "arrow-up",
    "sort_files": "arrow-up-down",
    "select_drive": "hard-drive",
    "init_db": "database",
    "config": "settings",
    "stats": "chart-column",
    "tree": "list-tree",
    "prune": "eraser",
    "mount": "plug",
    "umount": "unplug",
    "command_palette": "square-terminal",
    "help": "circle-question-mark",
    "quit": "power",
}

#: file kinds for the grid placeholder
KINDS = {
    "image": "image",
    "video": "film",
    "audio": "file-music",
    "unknown": "file-question-mark",
}


def _get(url: str) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "meret-icon-fetch"})
    with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
        return response.read()


def _latest_tag() -> str:
    """The newest release tag, so the default is never a stale guess."""
    payload = json.loads(_get(LATEST).decode("utf-8"))
    return payload["tag_name"]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", default=None, help="Lucide tag (default: latest release)")
    parser.add_argument("--out", type=Path, default=TARGET, help="target directory")
    args = parser.parse_args(argv)

    tag = args.tag or _latest_tag()
    args.out.mkdir(parents=True, exist_ok=True)
    print(f"lucide {tag} -> {args.out}")

    failed: list[str] = []
    for name, upstream in sorted({**COMMANDS, **KINDS}.items()):
        url = RAW.format(tag=tag, path=f"icons/{upstream}.svg")
        try:
            payload = _get(url)
        except urllib.error.HTTPError as error:
            failed.append(f"{name}: {upstream}.svg -> HTTP {error.code}")
            continue
        except urllib.error.URLError as error:
            failed.append(f"{name}: {upstream}.svg -> {error.reason}")
            continue
        (args.out / f"{name}.svg").write_bytes(payload)
        print(f"  {name}.svg  <- {upstream}.svg")

    # ISC: the notice has to travel with the files
    (args.out / "LICENSE").write_bytes(_get(RAW.format(tag=tag, path="LICENSE")))
    print(f"  LICENSE    <- lucide {tag}")

    if failed:
        print("\nfailed:", file=sys.stderr)
        for line in failed:
            print(f"  {line}", file=sys.stderr)
        print("\nfix the name in COMMANDS/KINDS, then re-run", file=sys.stderr)
        return 1
    print(f"\ndone. pin it: --tag {tag}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())