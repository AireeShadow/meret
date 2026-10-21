# meret 🐍

Tagged media library: tags instead of folders. Tags form a hierarchy
(`travel/beach`) that you browse like folders — in a terminal UI, from the
command line, or mounted as a read-only drive.

## Requirements

- Python 3.11+
- [FFmpeg](https://ffmpeg.org/)'s `ffprobe` on `PATH` for video and audio metadata
  (images are read with Pillow)
- For mounting: [WinFsp](https://winfsp.dev/) on Windows, FUSE on Linux/macOS

## Installation

```bash
pip install -e .          # everything: CLI, TUI, scanning, tags, search, mounts
pip install -e ".[dev]"   # + pytest, ruff, black, mypy
```

Textual, fsspec and `tomli-w` are regular dependencies; the mount driver is picked
by platform marker (`fusepy` elsewhere, `winfspy` on Windows). There is a `gui`
extra with PySide6 — see [GUI](#gui).

## Quick start

```bash
meret init ~/Media                          # creates ~/Media/meret.db
meret scan ~/Media --db ~/Media             # index images, video, audio
meret tag ~/Media/photo.jpg travel/beach --db ~/Media
meret search --tag travel --db ~/Media      # a tag also matches its subtags
meret tree --db ~/Media
meret tui --db ~/Media                      # browse it all interactively
```

The examples use a POSIX shell; on Windows give full paths (`C:\Users\you\Media`).

To drop `--db`, make the database the default:

```bash
meret config set paths.default_db ~/Media/meret.db
```

### Which database is used

Every command takes `--db` / `-d`: a directory containing `meret.db`, or a path to
a `.db` file. Without it, meret uses `$MERET_DB`, then `paths.default_db` from the
config, otherwise `./meret.db`. Only `meret init` creates a database (`--force`
recreates it); `meret where` shows which one a command would use.

## Scanning

```bash
meret scan ~/Media                  # index (or re-index) everything
meret scan ~/Media --skip-existing  # only new files and files whose size/mtime changed
meret scan ~/Media --dry-run        # read metadata and count, write nothing
meret scan ~/Media --workers 16     # metadata threads (default 8)
```

Supported formats:

| Type  | Extensions |
|-------|------------|
| image | jpg jpeg png gif bmp tiff webp heic |
| video | mp4 avi mkv mov webm flv wmv m4v (via `ffprobe`) |
| audio | mp3 wav flac aac ogg m4a wma opus (via `ffprobe`) |

Files that fail to read are still indexed, with the reason stored in their metadata
and written to the log. Ctrl+C while metadata is being read stops early and still
saves what was read so far.

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
while mounted (from the CLI or the TUI) are picked up without remounting — Explorer
may need F5. Stop it with Ctrl+C (or Enter, on Windows) in its terminal, or from
elsewhere with `meret umount M:`.

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

Not wired up yet — targeted at 0.7. Install the extra now to have PySide6 ready:

```bash
pip install -e ".[gui]"
```

`meret/gui` will hold the Qt code; `core`, `vfs` and `cli` stay importable without
PySide6, and the GUI will not depend on `cli`. The tag panel reuses
`meret.vfs.tree.VirtualTree`, so the GUI shows exactly what a mount would.

## Configuration

```bash
meret config path                          # where the file is
meret config show                          # everything, as TOML
meret config get paths.default_db
meret config set paths.default_db ~/Media/meret.db
meret config edit                          # open in your editor
meret config reset
```

The file is `config.toml` in your user config folder:

| OS | Location |
|----|----------|
| Windows | `%LOCALAPPDATA%\meret\config.toml` |
| Linux | `~/.config/meret/config.toml` |
| macOS | `~/Library/Application Support/meret/config.toml` |

Currently `paths.default_db` and `locale.language` are used. The `scan` and `gui`
sections (and `locale.date_format`) are stored but not read yet — use `scan`'s flags
instead.

## Language

Command output, the help texts and the TUI are available in English and Russian.

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
`meret/locales/messages.pot`.

Typer's own messages come from the stdlib gettext, not from these catalogs, so the
group in `meret/cli/app.py` replaces `resolve_command` and builds the message
itself.

`tests/test_locale_catalog.py` fails if a message in the code is missing from a
catalog, if a translation drops or renames a `{placeholder}`, or if a quoted
`'{cmd}'` loses a quote.

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
TUI, background threads or the WinFsp driver — are written to the log with a full
traceback.

## Development

```bash
pip install -e ".[dev]"
pytest
ruff check .
```

| Package | Contents |
|---------|----------|
| `meret/core` | models, SQLite repository and schema, scanner, metadata extractors, tag paths, config, locale, logging |
| `meret/vfs` | the virtual tree (tags → folders); backends: mount (WinFsp/FUSE) and fsspec |
| `meret/cli` | the Typer CLI, virtual-FS commands, the Textual TUI |

`core` doesn't import `vfs` or `cli`, and the virtual tree has no third-party
dependencies (`tests/vfs/test_architecture.py` checks both). The GUI adds a third
rule in `tests/gui/test_architecture.py`: Qt stays in `meret/gui`.

## Milestones

0.7 - GUI
0.6 - localization
0.5 - TUI
0.4 - QoL and bugfixes
0.3 - tag hierarchy
0.2 - virtual file system
0.1 - cli, total restructuring
0.0 - basic code

## License

GPL-3.0 — see [LICENSE](LICENSE).