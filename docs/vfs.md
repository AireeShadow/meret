# The virtual file system

The tag hierarchy becomes folders, and every file appears in the folder of each tag it
has. Nothing is moved or renamed on disk: a tag is a row, and the tree is a way of
looking at it.

`meret/vfs` has no third-party dependency of its own — the mount backends are optional
extras, and the tree itself is pure Python over the repository contract.

## The tree

`VirtualTree` turns tags into `Node`s: `name`, `vpath`, `is_dir`, `tag_id`,
`source_path`, `size`, `mtime`, `file_type`.

- **`listdir(vpath)` is the only way to ask** — there is no full materialisation of the
  library into memory. A folder listing is a query, and the same call serves the tag
  panel, the mount and the GUI grid, so the three cannot disagree.
- **Names are sanitized** for the filesystem: characters that are not allowed in a file
  name (`/\:*?"<>|`) show up as `∕`. The tree keeps the real tag path in `tag_id` and
  `vpath`, which is why a delete goes by tag id and not by the label.
- **Same-named files in one folder get `~1`, `~2` suffixes.** Two files called
  `photo.jpg` under `travel/beach` are both reachable, and the suffix is stable for a
  given listing.
- **`normalize()` is the single place** that turns `/travel/beach/` into `travel/beach`,
  so callers never compare paths they built differently.
- `real_path()` answers "where does this actually live", and raises `NotFound` or
  `IsADirectory` instead of returning a path that would fail later.

## Caching

The tree caches a snapshot, and invalidates it on `data_version()` — a number that moves
whenever another connection commits. That covers the CLI, the TUI, a mount and another
process, all with one comparison.

A backend that cannot answer returns `None`, and the cache falls back to a TTL. The GUI
polls `data_version` every 1.5 s; the panel is only re-read when it actually moved.

## Mounting

```bash
meret mount M:            # Windows (WinFsp)
meret mount ~/tags        # Linux/macOS (FUSE)
```

- **Read-only.** A tag hierarchy is a view, and a view that accepts writes needs a
  policy for "what does it mean to create a file in a tag folder".
- **Contents are read in ranges straight from the originals.** A preview of a 4 GB video
  reads a few kilobytes, not the file.
- **The mount process is found by mount point**, not by pid: `umount` scans for
  `meret mount` with the path in its command line. On Windows that is
  `Get-CimInstance Win32_Process`, which is slow and can hang — so the call has a
  timeout, and the GUI runs it off its thread.
- **Tag changes while mounted are picked up without remounting**, because the mount reads
  through the same cached tree. Explorer may need F5: it caches directory listings of
  its own.

## fsspec

The same tree is available as an fsspec filesystem, for notebooks and scripts:

```python
import fsspec
from meret.core.sqlite_repo import SQLiteRepository
from meret.vfs.backends.fsspec import register

register()
repo = SQLiteRepository("meret.db")
with fsspec.open("meret:///travel/beach/photo.jpg", "rb", repo=repo) as f:
    head = f.read(16)
```

Writes are refused, for the same reason the mount is read-only.
