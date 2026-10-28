# Changelog

Notable changes to meret. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
the version lives in `meret/__init__.py`.

Two rules: one file, one line per patch release unless the change needs explaining.
The repository has no git tags, so older entries are kept as a plain version log
rather than as comparison links.

## [0.7.30] — 2026-10-28

### Fixed

- **The filesystem panel showed only `C:` for about 21 seconds after start.** Not a
  lost drive list, a late one: Qt lists the drives on its own thread, `stat`s each one
  and hands them over in a single batch, and a disconnected network drive takes Windows
  ~21 s to give up on. It came and went because Windows caches that failure for a
  while. Each drive is now probed on its own thread and added as soon as it answers,
  so every reachable drive is on screen in about 0.1 s; only drives that cannot be
  reached still wait for Qt's batch.
- **The panel started sorted Z to A**, drives from the last letter down:
  `setSortingEnabled()` adopts the header's default descending indicator. It starts by
  name, ascending, matching the window's first sort mode.

### Changed

- Refreshing the panel probes the drives again, so a drive plugged in since start
  shows up; Qt reads the drive list only once per model.

## [0.7.29] — 2026-10-27

### Fixed

- **The window froze for seconds when the filesystem panel listed a directory.** The
  cause was not the layout of any dialog: `QFileSystemModel` watches every directory it
  has ever listed and re-reads a changed one on the GUI thread with a `stat` per entry
  — 6.4 seconds inside a single `QEvent`. The watcher is off by default now;
  `MERET_WATCH_FS=1` restores the stock model.
- **The panel showed only `C:`.** The drive list is the model's own root and only
  exists while the root path is empty; `reveal()` left the model rooted at the revealed
  folder. Navigation now reveals a path inside the tree instead of re-rooting it.
- **Every file came back untagged.** `in` on a `sqlite3.Row` tests values, not column
  names (cpython#100450), so `"tag_names" in row` was false for every real listing.
- **The suite hung in `pump()`**: `processEvents()` without a limit does not return
  while the event stream is alive. It is a nested `QEventLoop` with a timer now.
- **A watchdog thread raised `OSError: [WinError 6]`** on pytest's replaced stderr;
  the report goes to the file descriptor instead.
- A failing tag row kept looking empty on the next expand, and a settings dialog could
  open behind the window that modality had already blocked.

### Changed

- **Opening or recreating a database no longer blocks the GUI thread**: the open and
  the wipe (both retrying while a task in flight holds the file) run on the pool, and
  only the swap stays on the GUI thread.
- Revealing a folder no longer lists a path the model already knows, and already-open
  ancestors are not expanded twice.
- The settings dialog is a single `QScrollArea`, not tabs, so every field is reachable
  at once.

### Added

- `MERET_WATCHDOG=1` traces a run: every thread's stack every 3 s, plus an event rate,
  the slowest event and its widget chain.
- `docs/gui.md` — why the Qt layer is shaped the way it is.
- Badges in the README.

## Version log

Plain one-line entries, newest first, carried over from the old `CHANGELOG`. From
0.7.28 down they are historical and unedited.

- 0.7.28 - test fixes
- 0.7.27 - pool for open and wipe
- 0.7.26 - fix for tests
- 0.7.25 - ruff checks fixes (part 3)
- 0.7.24 - ruff checks fixes (part 2)
- 0.7.23 - readme and ruff check (part 1)
- 0.7.22 - descriptor fix
- 0.7.21 - fix for slow fs panel
- 0.7.20 - fix for test freeze
- 0.7.19 - debugging test freezes
- 0.7.18 - real FS fix for GUI
- 0.7.17 - configuration initial
- 0.7.16 - modal window for DB
- 0.7.14 - text menu
- 0.7.13 - _pump overflow fix, removing open_file command and icons
- 0.7.12 - removing redundant commands from GUI
- 0.7.11 - full filesystem preview
- 0.7.10 - more tests, couple fixes
- 0.7.9 - async tag tree, cli call tests, help coverage
- 0.7.8 - vendored lucide icons
- 0.7.7 - command pallete fixes
- 0.7.6 - command pallete
- 0.7.5 - video preview
- 0.7.4 - thumbnail bugfix
- 0.7.3 - small bugfixes
- 0.7.2 - fixed gui issues
- 0.7.1 - working bugged gui!
- 0.7.0 - gui initial
- 0.6.15 - moved locale and cli tests
- 0.6.14 - a lot of fixes to locale, tests and tui
- 0.6.13 - po deduplication and other fixes
- 0.6.12 - fix for locale tests and translation
- 0.6.11 - locale debug
- 0.6.10 - varius fixes and polishing
- 0.6.9 - tests for tui locale and final untranslated leftovers
- 0.6.8 - command pallete fix
- 0.6.7 - more translations
- 0.6.6 - more tui translation
- 0.6.5 - tui translation
- 0.6.4 - trying to translate typer
- 0.6.3 - fixing setting up locale from config
- 0.6.2 - third itearation of locale
- 0.6.1 - second locale iteration
- 0.6.0 - initial locale feature
- 0.5.11 - verbose logger fix
- 0.5.10 - various fixes
- 0.5.9 - readme covers all commands, tui, mounting, config and logging; ls/open/stat/mount/umount registered in the cli again
- 0.5.8 - logging: one log file in the user log dir, -v/--log-level, `meret logs`, traced repo calls, crash tracebacks in the log
- 0.5.7 - tui: working '..' navigation, virtual panel back beside the real one, command palette runs commands, fixed U/d/tree and help
- 0.5.6 - trying to do it again
- 0.5.5 - trying to implement ..
- 0.5.4 - open file command
- 0.5.3 - wrong panel focus fix
- 0.5.2 - tui tests
- 0.5.1 - fix for vfs rendering in tui
- 0.5.0 - intermediate TUI
- 0.4.7 - vfs: cached tag tree and listings (refreshed on db changes), ranged file reads instead of whole-file loads, real file dates in mounts
- 0.4.6 - fixes for config
- 0.4.5 - various fixes
- 0.4.4 - fix for new sql schema
- 0.4.3 - debugging fingerprints
- 0.4.2 - formatting
- 0.4.1 - search fix
- 0.4.0 - translated code comments, docs, tests and CLI messages from Russian to English
- 0.3.7 - attribute error fix
- 0.3.6 - non-unique children of parents
- 0.3.5 - logic refactoring
- 0.3.4 - adding an option to skip existing files
- 0.3.3 - fix for automatic db creation
- 0.3.2 - shitload of tag fixes and fixes in the tests
- 0.3.1 - added lost lines
- 0.3.0 - initial tag hierarchy
- 0.2.16 - race fix
- 0.2.15 - attempt to fix zombie
- 0.2.14 - debugging umount
- 0.2.13 - adding unmount
- 0.2.12 - updated tests for indexer
- 0.2.11 - adding progress bar for metadata collection
- 0.2.10 - fix for broken or incomplete files
- 0.2.9 - adding scan progess bar
- 0.2.8 - fix for initializing db from cli
- 0.2.7 - fix for zero file size
- 0.2.6 - fixes for mounts
- 0.2.5 - fix for winfspy
- 0.2.4 - various fixes for vfs
- 0.2.3 - adding mount command and its deps
- 0.2.2 - adding missing __init__.py
- 0.2.1 - fixes for fs and tests
- 0.2.0 - inital vfs
- 0.1.6 - fixing search crash
- 0.1.5 - fixing critical bugs
- 0.1.4 - adding tests
- 0.1.3 - refactoring
- 0.1.2 - restructuring, part 2
- 0.1.1 - restructuring, part 1
- 0.1.0 - cli and refactoring
- 0.0.3 - indexer
- 0.0.2 - common db operations class
- 0.0.1 - basic db operations class
- 0.0.0 - initial commit