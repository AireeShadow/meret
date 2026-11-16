# meret 🐍

![license](https://img.shields.io/badge/license-GPL--3.0-blue)
![python](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue)
![version](https://img.shields.io/badge/version-0.10.0-blue)
![tests](https://img.shields.io/badge/tests-passing-brightgreen)
![lint](https://img.shields.io/badge/lint-ruff-orange)
![platform](https://img.shields.io/badge/platform-Windows%20%7C%20Linux%20%7C%20macOS-lightgrey)

Tagged media library: tags instead of folders. Tags form a hierarchy
(`travel/beach`) that you browse like folders — in a terminal UI, from the command
line, in a Qt window, or mounted as a read-only drive.

One database, four ways in. Nothing is ever moved or renamed on disk: a tag is a row,
and the hierarchy is only a way of looking at it.

See [CHANGELOG.md](CHANGELOG.md) for what changed, and [docs/gui.md](docs/gui.md) for
how the Qt layer is built.

## Documentation

| File | About |
|------|-------|
| [docs/core.md](docs/core.md) | models, schema, the repository contract, the indexer, extractors, config, debug flags, locale, logging |
| [docs/autotag.md](docs/autotag.md) | the rule language: facts, operators, templates, the root, and why rules are a file |
| [docs/vfs.md](docs/vfs.md) | the virtual tree, caching, mounting and fsspec |
| [docs/cli.md](docs/cli.md) | the Typer app, the locale trap, and the TUI |
| [docs/gui.md](docs/gui.md) | why the Qt window is shaped the way it is, and what each shape cost |

## Requirements

- Python 3.11+
- [FFmpeg](https://ffmpeg.org/)'s `ffprobe` on `PATH` for video and audio metadata
  (images are read with Pillow)
- For mounting: [WinFsp](https://winfsp.dev/) on Windows, FUSE on Linux/macOS

## Installation

```bash
pip install -e .            # CLI, TUI, scanning, tags, search, autotagging, mounts
pip install -e ".[gui]"     # + the Qt window (PySide6)
pip install -e ".[dev]"     # + pytest, ruff, black, mypy
```

Textual, fsspec and `tomli-w` are regular dependencies; the mount driver is picked by
platform marker (`fusepy` elsewhere, `winfspy` on Windows). PySide6 is an extra: the
GUI is the only thing that needs it, and `core`, `vfs` and `cli` stay importable
without it.

## Quick start

```bash
meret init ~/Media                          # creates ~/Media/meret.db
meret scan ~/Media --db ~/Media             # index images, video, audio
meret tag ~/Media/photo.jpg travel/beach --db ~/Media
meret search --tag travel --db ~/Media      # a tag also matches its subtags
meret autotag --dry-run --db ~/Media        # what the built-in rules would add
meret autotag --db ~/Media                  # ...and then for real
meret tree --db ~/Media
meret tui --db ~/Media                      # browse it all interactively
meret gui --db ~/Media                      # ...or in a window
```

The examples use a POSIX shell; on Windows give full paths (`C:\Users\you\Media`).

To drop `--db`, make the database the default:

```bash
meret config set paths.default_db ~/Media/meret.db
```

### Which database is used

Every command takes `--db` / `-d`: a directory containing `meret.db`, or a path to a
`.db` file. Without it, meret uses `$MERET_DB`, then `paths.default_db` from the
config, otherwise `./meret.db`. Only `meret init` creates a database (`--force`
recreates it, dropping `-wal` and `-shm` with it); `meret where` shows which one a
command would use.

## Scanning

```bash
meret scan ~/Media                  # index (or re-index) everything
meret scan ~/Media --skip-existing  # only new files and files whose size/mtime changed
meret scan ~/Media --dry-run        # read metadata and count, write nothing
meret scan ~/Media --workers 16     # metadata threads (default 8)
meret scan ~/Media --autotag        # ...and run the auto-tagging rules afterwards
```

Supported formats:

| Type  | Extensions |
|-------|------------|
| image | jpg jpeg png gif bmp tiff webp heic |
| video | mp4 avi mkv mov webm flv wmv m4v (via `ffprobe`) |
| audio | mp3 wav flac aac ogg m4a wma opus (via `ffprobe`) |

Files that fail to read are still indexed, with the reason stored in their metadata
and written to the log. Ctrl+C while metadata is being read stops early and still
saves what was read so far. In the GUI the same is a **Cancel** button: what was
written stays written, because a scan is not a transaction.

## Tags

Tags are paths: `genre/mystery` is the tag `mystery` under `genre`. Missing parents
are created on the way. The same name may live in different branches
(`genre/mystery`, `studio/mystery`), so use full paths for nested tags: when adding,
a bare name means a root-level tag (`meret tag FILE mystery` creates a root `mystery`
even if `genre/mystery` exists). When looking a tag up (search, `parent`, `tag-del`),
a bare name finds the root-level tag first, then the oldest namesake.

```bash
meret tag FILE travel/beach family        # add tags to an indexed file
meret tag FILE family --remove            # remove one
meret tag-new genre/mystery --colour red  # create a tag (and its parents)
meret parent family travel                # move a tag under another
meret parent family                       # ...or back to the root
meret tag-del genre                       # delete; its subtags move up a level
meret tags                                # all tags as paths, with file counts
meret tree                                # the hierarchy as a tree
meret tag-prune                           # delete tags with no files and no subtags
meret stats                               # files per type, tag count
```

## Auto-tagging

A rule is a tag path plus conditions over a file's **already indexed** metadata — no
disk is touched, no EXIF is re-read: everything comes from the `metadata` column.

```bash
meret autotag --dry-run                    # what would change, nothing written
meret autotag                              # apply the rules to the whole library
meret autotag --rules ~/Media/rules.toml   # ...with a file instead of the config
meret autotag --root library --only-untagged --limit 100
```

The rules live in a TOML/JSON file, named by `autotag.rules_file` or `--rules`.
**Nine rules ship with meret**, so a fresh install does something instead of printing
"no rules": kind, format, orientation, two resolution buckets, year, camera make, long
video, screenshot. A configured file that is missing stays missing, with a warning —
falling back to a default policy would tag a library nobody asked it to tag.

```toml
[[rules]]
tag = "camera/{exif.Make}"

[[rules.conditions]]
field = "exif.Make"
op = "exists"

[[rules]]
tag = "video/long"

[[rules.conditions]]
field = "duration_s"
op = "gte"
value = 600
```

**Facts** are a flat view of the metadata: dotted keys such as `exif.Make`,
`tags.artist`, `creation_time`, plus derived ones — `type`, `orientation`
(`landscape`/`portrait`/`square`), `aspect`, `megapixels`, `duration_s`,
`duration_min`, `year`, `stem`, `extension`.

**Operators:** `eq ne in not_in contains startswith endswith regex gt gte lt lte
exists missing`. `match = "all"` (default) or `"any"` per rule, and a rule may carry a
`colour`.

**Templates:** `{field}` in a tag is filled from the facts, and a rule whose
placeholder has no value is skipped — `camera/{exif.Make}` never produces a bare
`camera/`. EXIF strings are stripped, so `"Canon "` becomes `camera/Canon`.

**The parent tag** is `autotag.root`, `metadata` by default, so the results never mix
with hand-made tags at the top of the tree: `camera/{exif.Make}` becomes
`metadata/camera/Canon`. An empty root files them bare, and a rule that already spells
the root out is not prefixed twice.

Auto-tagging is available from all three front-ends: `meret autotag`, `a` in the TUI,
and `Ctrl+Shift+A` (or the toolbar) in the window — both with a progress window and a
Cancel button.

## Search

```bash
meret search beach                         # by file name (word prefix)
meret search --tag travel                  # tag and all its subtags
meret search --tag travel --tag family     # any of them
meret search --tag travel --tag family --all
meret search --tag travel --type video     # image | video | audio
```

With `--tag`, the name text is not used.

## Virtual file system

The tag hierarchy becomes folders, and every file appears in the folder of each tag
it has. Same-named files in one folder get `~1`, `~2` suffixes; characters that
aren't allowed in file names (`/\:*?"<>|`) show up as `∕`.

```bash
meret ls /                          # top-level tags
meret ls /travel/beach --long       # with sizes
meret stat /travel/beach/photo.jpg  # what it is, where it lives on disk
meret open /travel/beach/photo.jpg  # open with the default app (--path: just print it)
```

`meret open-file` is an older variant of `meret open`.

### Mounting

```bash
meret mount M:            # Windows (WinFsp) — a drive letter
meret mount ~/tags        # Linux/macOS (FUSE) — a directory
```

The mount is read-only. File contents are read in ranges straight from the
originals, so previews of large videos don't load whole files. Tag changes made
while mounted (from the CLI, the TUI or the GUI) are picked up without remounting —
Explorer may need F5. Stop it with Ctrl+C (or Enter, on Windows) in its terminal, or
from elsewhere with `meret umount M:`.

### fsspec

```python
import fsspec
from meret.core.sqlite_repo import SQLiteRepository
from meret.vfs.backends.fsspec import register

register()
repo = SQLiteRepository("meret.db")
with fsspec.open("meret:///travel/beach/photo.jpg", "rb", repo=repo) as f:
    head = f.read(16)
```

## Terminal UI

```bash
meret tui --db ~/Media
```

Left: the real file system. Right: the tags as folders. Below: a preview and the
current selection.

| Key | Action |
|-----|--------|
| ↑ ↓, Enter | move; open a file / expand a folder |
| `..`, `u`, Backspace | parent folder (at a drive root on Windows: pick a drive) |
| `h`, `~` | home folder |
| `D` | pick a drive (Windows) |
| `S` | cycle sorting: name → size → date (folders stay on top) |
| `t` | tag the selected real file |
| `a` | auto-tag the library with the configured rules |
| `d` | delete the tag under the cursor in the tags panel (asks y/n) |
| `s` | scan the selected real folder |
| `/` | search: a name, or `tag:travel` |
| `:` | command palette (type a command, Enter) |
| `m` | mount the tags (inside the TUI) |
| `U` | stop a mount started with `meret mount` |
| `r` | refresh both panels |
| `c`, `i` | edit config, init the database |
| `?`, `q` | help, quit |

A mount started with `m` lives in the TUI's process: `U` can't find it, and it ends
when the TUI exits. For a mount you can stop on its own, run `meret mount` in a
separate terminal.

## GUI

```bash
pip install -e ".[gui]"
meret gui --db ~/Media
```

The window is the TUI's layout in Qt: the tag tree above the real file system on the
left, a thumbnail grid in the middle, a preview on the right, the selection count in
the status bar.

- **The real-FS panel is rooted at exactly one folder**, and a selector above it
  chooses which. The panel used to show one tree from the top of the filesystem, and
  that root is the drive list: building it asks Windows for every drive's label and
  free space, and a disconnected mapped drive answers after ~5.5 s — on the GUI thread,
  in a single event, which froze the window. The drive letters come from
  `GetLogicalDrives()`, a bitmask read from the mount manager, so no drive is ever
  opened just to list it.
- **Every TUI command is here**, with the same descriptions: on the toolbar (icons,
  with the name and key in the tooltip), in the menu bar, and in the palette on `:`,
  which shows each command with its key. `tests/gui/test_commands.py` compares the two
  tables, so the GUI and the TUI cannot drift apart.
- **Long runs get a progress window**: scanning shows the indexer's phases, auto-tagging
  shows files processed against the library's size. Both have a Cancel button, and both
  refuse to be closed while the work runs — cancelling asks the work to stop on a file
  boundary instead of leaving a window behind it.
- **No `home` / `up` / `select_drive` / `open_file`**: the drive selector and the panel
  cover navigation, and a double click opens a file.
- **Thumbnails** are decoded off the GUI thread (Pillow for images, a single ffmpeg
  frame for video) and cached on disk, keyed by database, file and mtime. A file with
  no preview keeps a type icon, and its tooltip says why.
- **Nothing touches SQLite on the GUI thread**: repository calls and decodes go through
  a thread pool and come back as signals, each carrying a generation, so a stale result
  cannot paint over a newer one.
- **Mounting shells out** to `meret mount` / `meret umount`, so opening a window never
  imports FUSE or WinFsp.
- **Settings are a dialog**, not a text editor: `Ctrl+,` (or File → config) opens every
  config field as a checkbox, a spin box, a drop-down or a path with a browse button,
  with the numeric bounds taken from the pydantic model. Ok writes and applies, Cancel
  changes nothing, and a button opens the file for hand edits.

| Key | Action |
|-----|--------|
| F5 | refresh |
| F6 | scan the folder selected in the real-FS panel |
| Ctrl+F | search: a name, or `tag:travel` |
| Ctrl+T / Ctrl+Shift+T | tag / untag the selection |
| Ctrl+Shift+A | auto-tag the library with the configured rules |
| Ctrl+D | delete the tag under the cursor in the tag tree |
| Alt+S | cycle sorting: name → size → date |
| Ctrl+I | initialize a new database (asks where, if one exists) |
| Ctrl+, | settings |
| Alt+I / Alt+L / Alt+P | statistics / tag tree / prune orphan tags |
| Alt+M / Alt+U | mount / unmount |
| `:` | command palette |
| F1 | help |
| Ctrl+Q | quit |
| ← / → | in the viewer: 10 s back / forward in a video, previous / next file otherwise |
| Space, Enter | in the viewer: play or pause |

### When the window misbehaves

`--debug` turns diagnostics on, and it goes **before** the command name:

```bash
meret --debug watchdog gui
meret --debug watchdog,watch_fs gui
```

| Name | What it does |
|------|--------------|
| `watchdog` | every thread's stack every 3 s, plus an event rate and the slowest event |
| `watch_fs` | keep the filesystem watcher on (the stock `QFileSystemModel`) |
| `settings_modeless` | settings as a modeless window |
| `media_log` | put the FFmpeg backend's log back |
| `no_tooltips` | drop the toolbar tooltips, for an A/B run |

A misspelled name is a command-line error listing the real ones — which is the whole
reason the flags exist: `MERET_WATCHDOGG=1` used to be a silent no-op. The environment
variables still work as a fallback; the flag wins when both are present.

`watchdog` prints a line a second with the event rate, the slowest event and who holds
the focus, from a separate thread — so the output keeps coming while the GUI thread is
the thing that is stuck. A rate of `0 events/s` means the thread is blocked outside
event handling, not looping, and `stuck for N s in: <widget>/<event>` names the event
that never returned.

`media_log` is about output rather than stalls: the FFmpeg backend normally logs its
version, codecs and every container misdetection to the terminal, and a window has no
console for any of it. The rule is set before `QApplication` is created, because Qt
reads it at construction.

## Configuration

```bash
meret config path                          # where the file is
meret config show                          # everything, as TOML
meret config get paths.default_db
meret config set paths.default_db ~/Media/meret.db
meret config set autotag.rules_file ~/Media/rules.toml
meret config edit                          # open in your editor
meret config reset
```

The file is `config.toml` in your user config folder:

| OS | Location |
|----|----------|
| Windows | `%LOCALAPPDATA%\meret\config.toml` |
| Linux | `~/.config/meret/config.toml` |
| macOS | `~/Library/Application Support/meret/config.toml` |

`paths.default_db` and `locale.language` are read by the CLI, and the GUI reads the
`gui` section: theme, thumbnail size and window size are applied when the settings
dialog is accepted (a theme is only re-applied when it actually changed — re-polishing
the app is a visible stall). `scan` (`workers`, `chunk_size`, `skip_existing`) and
`locale.date_format` are stored and editable in the dialog, but not read yet — use the
`scan` flags.

The `autotag` section holds the auto-tagging policy — `enabled` (the default for
`scan --autotag`), `only_untagged`, `root` and `rules_file`. The rules themselves are
not in the config: they are a list of lists, and the settings dialog shows exactly one
widget per config field, which a rules table is not.

## Language

Command output, the help texts, the TUI and the GUI are available in English and
Russian.

```bash
meret --lang ru scan ~/Media             # this run only (-l ru)
meret config set locale.language ru      # from now on
meret config set locale.language auto    # follow the system (the default)
```

The first match wins: `--lang`, then `locale.language` in the config, then the
system language (`LANGUAGE`/`LC_ALL`/`LANG`, else the Windows display language),
then English. An unknown language falls back to English. `--lang` goes before the
command name.

`--lang` is read from `sys.argv` while `meret/cli/app.py` is being imported, because
Typer freezes every `help=` string the moment the decorators run. A stray
`app = typer.Typer(...)` later in that module would drop `cls=TranslatedGroup` and
silently un-translate the "no such command" error —
`tests/test_locale_catalog.py::test_app_uses_the_translated_group` guards that.

### Translating

Translations are plain gettext catalogs, read directly — no compile step:
`meret/locales/<lang>/LC_MESSAGES/messages.po`. To add a language, copy the Russian
file, change `Language:` and `Plural-Forms:` in its header, and translate the
`msgstr` lines. In code, keep markup and values out of the message:

```python
msg = _("Database created: {path}").format(path=db_file)
console.print(f"[green]✓ {msg}[/green]")
```

`python scripts/extract_strings.py` lists every translatable string in
`meret/locales/messages.pot`. Run it before the tests, not after them: it names the
missing and the orphaned `msgid`s that `tests/test_locale_catalog.py` would fail on.

Typer's own messages come from the stdlib gettext, not from these catalogs, so the
group in `meret/cli/app.py` replaces `resolve_command` and builds the message
itself.

`tests/test_locale_catalog.py` fails if a message in the code is missing from a
catalog, if a translation drops or renames a `{placeholder}`, if a quoted `'{cmd}'`
loses a quote, if a `msgid` appears twice, or if a catalog entry nothing asks for is
left behind.

## Logging and debugging

meret writes one log file (rotated at 5 MB, 5 old files kept):

| OS | Location |
|----|----------|
| Windows | `%LOCALAPPDATA%\meret\Logs\meret.log` |
| Linux | `~/.local/state/meret/log/meret.log` |
| macOS | `~/Library/Logs/meret/meret.log` |

```bash
meret logs                           # print the path
meret logs -n 50                     # ...and the last 50 lines
meret -v scan ~/Media                # DEBUG, also echoed to stderr
meret --log-level WARNING scan ...   # quieter (DEBUG, INFO, WARNING, ERROR)
MERET_LOG_LEVEL=DEBUG meret tui      # the TUI logs to the file only
MERET_LOG_FILE=./debug.log meret ... # log somewhere else
```

`-v` and `--log-level` go before the command name. The default level is INFO: each
command line, scan phases and timings, warnings and errors. At DEBUG you also get
every database call with its arguments, result size and duration, how the database
path was chosen, and virtual-FS cache and mount activity. Crashes — in the CLI, the
TUI, the GUI, background threads or the WinFsp driver — are written to the log with a
full traceback.

The GUI logs to the file only (a window has no console). `meret gui` is the one place
where the log is the first thing to read when something looks stuck.

## Development

```bash
pip install -e ".[dev]"
pytest
ruff check .
ruff format --check .
mypy meret
```

| Package | Contents |
|---------|----------|
| `meret/core` | models, SQLite repository and schema, scanner, metadata extractors, tag paths, auto-tagging rules and the tagger, config, debug flags, locale, logging |
| `meret/vfs` | the virtual tree (tags → folders); backends: mount (WinFsp/FUSE) and fsspec |
| `meret/cli` | the Typer CLI, virtual-FS commands, the Textual TUI |
| `meret/gui` | the Qt layer: the window, the panels, the settings dialog, the worker pool |

`meret/gui/app.py` is the assembly, not the window: it holds `MainWindow` and
`run_gui`. Everything else is a module with one reason to change.

| File | Responsible for |
|------|-----------------|
| `gui/window/base.py` | the state, the first paint, the shutdown, and the mixin contract |
| `gui/window/layout.py` | panels, grid, preview, toolbar, menus |
| `gui/window/contents.py` | listings, thumbnail batches, preview, search, the status line |
| `gui/window/commands.py` | the command handlers, the settings dialog, the help |
| `gui/window/databases.py` | opening, wiping and switching the database |
| `gui/window/mounts.py` | the shell-outs to `meret mount` / `meret umount` |
| `gui/window/polling.py` | the poll that notices writes from other processes |
| `gui/window/constants.py` | the toolbar order and the icon size |
| `gui/window/progress.py` | the modal progress window and its Cancel button |
| `gui/fs_panel.py`, `gui/tag_tree.py` | the two panels |
| `gui/diagnostics.py` | the traced application and the media-log switch |
| `gui/drives.py` | the drive letters, from `GetLogicalDrives()` |
| `gui/theme.py` | the palette |
| `gui/thumbnails.py`, `gui/viewer.py`, `gui/workers.py`, `gui/icons.py` | decoding, playback, the pool, the glyphs |

`MainWindow` is assembled from mixins, and each of them is a plain class at runtime
and a `WindowBase` subclass under `TYPE_CHECKING`. That is what lets mypy see the
attributes and the Qt methods they share, without changing the runtime MRO —
`WindowBase` is listed last for exactly this reason.

Four rules keep the layers apart, each with a test behind it:

- `core` doesn't import `vfs` or `cli`, and the virtual tree has no third-party
  dependencies (`tests/vfs/test_architecture.py`).
- Qt stays in `meret/gui`; the GUI does not import the mount backends
  (`tests/gui/test_architecture.py`).
- The GUI's command table is the TUI's, minus the four commands the whole-filesystem
  panel replaced (`tests/gui/test_commands.py`).
- Every config field has a widget in the settings dialog
  (`tests/gui/test_settings.py`), so no setting can be silently unreachable.

[`docs/gui.md`](docs/gui.md) explains why the window is built the way it is — the
thread pool and its generations, the filesystem panel without a watcher, the deferred
`raise_()`, and the traps that were paid for with a freeze.

The GUI test suite runs headless (`QT_QPA_PLATFORM=offscreen`) and stubs every modal
dialog: a dialog runs its own event loop, and nothing clicks in a headless run.

## Milestones

- 0.9 - metadata auto-tagging, GUI refactoring (current)
- 0.8 - internal viewer
- 0.7 — GUI
- 0.6 — localization
- 0.5 — TUI
- 0.4 — QoL and bugfixes
- 0.3 — tag hierarchy
- 0.2 — virtual file system
- 0.1 — cli, total restructuring
- 0.0 — basic code

## License

GPL-3.0 — see [LICENSE](LICENSE).
