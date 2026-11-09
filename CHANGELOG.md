# Changelog

Notable changes to meret. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
the version lives in `meret/__init__.py`.

Two rules: one file, one line per patch release unless the change needs explaining.
The repository has no git tags, so older entries are kept as a plain version log
rather than as comparison links.

## \[0.8.4\] — 2026-11-05

### Fixed

- **Three tests asserted a spelling only one platform produces**, so the Linux leg of
  the matrix was red while a Windows checkout was green.
  - `test_vfs_commands_are_registered_in_the_real_cli` looked for `" ls "` in
    `meret --help`. Rich draws the help inside a panel, so on a narrow terminal a
    command sits against a border or wraps and the spaces around it are not fixed.
    The ANSI-stripped output is matched word by word now.
  - `test_a_slow_drive_does_not_hold_back_the_others` spelled the fake drive `Q:/`,
    but `QFileInfo("Q:/").absolutePath()` resolves against the working directory on
    Unix (`.../Q:`), so the probe never saw the string the test wrote and the "slow"
    drive answered at once. Both the fake probe and the assertions match on `"Q:"`.
  - `test_load_existing_json_config` asserted `workers == 16`, while
    `ScanConfig.validate_workers` caps the value at `cpu_count() * 2`. The
    expectation is computed with the same cap now, so a four-core runner no longer
    fails a config that loaded correctly.
  - **The Windows leg could not install at all.** `winfspy` is an sdist that compiles
    CFFI bindings, and it looks for the WinFsp SDK through `WINFSP_LIBRARY_PATH` or the
    `HKLM\SOFTWARE\WinFsp` registry key; a runner has neither, so `pip install` died in
    `get_winfsp_dir`. CI installs WinFsp and sets the variable — a local machine with
    WinFsp already installed never showed the problem.
  - `test_init_db_recreates_the_database_here` raced the wipe. The predicate read
    `get_all_tags()` while the tables were already dropped and the repository not yet
    reopened, so the old repository answered `no such table: tags` and the wait failed
    on the way to a state it would have reached a moment later. The gap is now tolerated
    instead of treated as the answer.

### Changed

- `asyncio_default_fixture_loop_scope = "function"` is set in
  `[tool.pytest.ini_options]`. `pytest-asyncio` prints a deprecation warning on every
  run without it, and the warning sat in the middle of the CI log where a real one
  would be missed. `function` is the scope the plugin will default to.

## \[0.8.3\] — 2026-11-05

### Changed

- **The advisory `mypy` job is green.** 61 findings, worked down by root rather than
  one `type: ignore` at a time: modules without stubs are `ignore_missing_imports`,
  the Typer and Textual override codes are relaxed where the bindings describe their
  own internals, and the rest was real. `TagTree` and `MainWindow._tree` are typed
  `VirtualTree | None` because `set_tree(None)` is a real state, and `QApplication`
  is narrowed from `QCoreApplication` instead of being cast at two call sites.
- **`sqlite3.Row` types every column `int | None`, and that infected the whole
  graph.** Ids are coerced where a row becomes a model (`_row_to_file`, `get_all_tags`,
  `get_tag_usage`, `_tag_id`, `_ensure_tag`, `save_file`), so `tagpaths`, `vfs/tree`,
  `cli/app` and the GUI stopped carrying `int | None` through every signature.
  `tagpaths.path_of` takes `dict[int | None, Tag]` for the same reason: `Tag.id` is
  optional by design, and a path walk starts at the tag, not at its key.
- Lambdas handed to `TaskRunner.run` and `QTimer.singleShot` became nested functions
  and `functools.partial`. `run` takes `Callable[..., Any]`, so mypy had nothing to
  infer a lambda's parameters from — and an untyped lambda is where a wrong argument
  order hides, which is exactly what happened to `failed` in `_pump`.

### Fixed

- `QPixmap.fromImage` is only called with a real image: a `Thumbnail` carries
  `QImage | None`, and both `_set_icon` and `_set_preview` checked `ok` alone.
- `_iter_statements` handles the impossible `None` from `_WORD.match` instead of
  assuming it, and `defined[ident]` in the `.po` parser takes an `int`.
- **Two GUI tests waited on time instead of on an effect.** `test_db_switch`'s fixture
  blocked on the task pool (`wait()`), so the start-up listing no longer races a
  guessed budget, and the failed-listing test pumps until the row appears.
  `test_window`'s `fake_run` stub assumed every task has an argument; the version
  probe has none.

## \[0.8.2\] — 2026-11-05

### Changed

- **The FFmpeg backend no longer writes to stderr.** Opening the window printed the
  backend's version, its whole codec table and every container misdetection into the
  terminal that started it — a window has no console of its own, so that output had
  nowhere useful to go and buried the `meret` log commands next to it. The noise is
  turned off with `QT_LOGGING_RULES` (`qt.multimedia.ffmpeg*.info=false` and
  `*.debug=false`), set by `quiet_media_logs()` **before** `QApplication` is created:
  the rules are read at application construction, and a rule applied later misses the
  plugin's own start-up lines. A rule already in the environment is appended to, never
  replaced, so a developer's own `QT_LOGGING_RULES` survives.
- Only `info` and `debug` are silenced. `warning` and `critical` still come through:
  a file that will not open, and the reason, is exactly the line worth reading.
- `MERET_MEDIA_LOG=1` restores the whole `qt.multimedia.*` category for a run. It is
  the escape hatch when playback misbehaves and the codec negotiation has to be seen;
  documented next to the other three variables.

### Added

- `tests/gui/test_media_logs.py` coverage, in `test_viewer.py`: the rule is set by
  default, an existing rule is appended to, and `MERET_MEDIA_LOG` overrides both.
- A `video_file` fixture builds a real one-second clip with `ffmpeg` (`lavfi` colour
  source, `yuv420p`) and skips when `ffmpeg` is not on `PATH`. Two tests open a video
  and an empty `.mp4` made them print `moov atom not found` on every run.

### Fixed

- The suite no longer prints FFmpeg's container guessing. `tests/gui/conftest.py` sets
  the same logging rule next to `QT_QPA_PLATFORM`, before the first Qt import, so the
  noise cannot be re-enabled by import order.

## \[0.8.1\] — 2026-11-05

### Added

- **Playback controls under the viewer**: previous file, play/pause, stop, next file,
  a scrub bar and an `m:ss / m:ss` clock. The glyphs are vendored Lucide files
  (`play`, `pause`, `square`, `skip-back`, `skip-forward`) added to the `GLYPHS` table
  in `scripts/fetch_icons.py`, so `prune()` keeps them.
- `test_every_control_glyph_is_vendored`, so a missing file is a red test instead of a
  silent letter tile.

### Fixed

- **The scrub bar could not be dragged.** `positionChanged` repainted the handle under
  the pointer on every player tick, so the drag looked dead and the seek never landed.
  `sliderPressed`/`sliderReleased` bracket the drag now, playback is ignored while it
  lasts, and one `setPosition` runs on release.
- Stop rewinds: `QMediaPlayer.stop()` alone leaves the position where it was, so the
  next Play resumed mid-file.
- **The media backend is built on the first video, not with the window.** Creating a
  `QMediaPlayer` starts a backend, and the start-up fixture in `tests/gui/test_db_switch.py`
  is timing-sensitive enough for that to matter.

### Changed

- Control glyphs are re-tinted after a theme change (`ViewerView.retint()`, with
  `icons.clear_cache()`), so they follow the palette like the toolbar glyphs do.

## \[0.8.0\] — 2026-11-05

### Added

- **A viewer tab.** The central widget is a `QTabWidget`: `Browser` holds the panels,
  the grid and the preview exactly as before, `Viewer` shows one file large — an image
  in a `QLabel`, a video through `QMediaPlayer` with play/pause and a position slider.
  A double-click on a file opens it there instead of handing it to the system.
- `meret/gui/viewer.py`: `ViewerView`, the page itself. `VIDEO_SUFFIXES` decides what
  is played; everything else goes to `QPixmap`. Images are re-fitted on resize, and a
  file that will not decode names itself instead of leaving the page blank.
- `tests/gui/test_viewer.py`: the tab count and order, the double-click paths, the
  broken-image message, and playback stopping when the tab is left.
- Locale entries for `Browser`, `Viewer`, `Nothing opened`, `Cannot open {path}`,
  `Play` and `Pause` in both catalogs.

### Changed

- `QAction` is imported from `QtGui`. Qt 6 moved it there, and PySide6 keeps no
  alias in `QtWidgets`, so an import from the wrong module fails at start-up and the
  CLI reports it as a missing PySide6.
- The grid gets a minimum width and the splitter gets its stretch factors back: with
  all three at zero, becoming a tab page collapsed the grid and the preview to zero
  width — the browser looked empty and a white strip sat at the top.
- `setSizes` is re-asserted once after the first layout pass, since a page has no real
  width before it.

### Fixed

- Leaving the viewer stops playback, so a hidden video keeps no sound and no source.

## \[0.7.32\] — 2026-10-29

### Added

- **`scripts/sync_version.py` and `tests/test_version.py`.** The README badge was a
  second spelling of `meret/__init__.py` and nothing kept the two together. The script
  rewrites the badge, `--check` reports a drift without writing, and the test runs the
  check — so a version bump that forgets the badge is a red suite instead of a diff
  nobody reads. `test_version.py` also fails when the changelog has no section for the
  current version.
- **`.pre-commit-config.yaml`**: trailing whitespace, end-of-file, line endings,
  YAML and TOML validity, oversized files, `ruff-check --fix`, `ruff-format`, the
  badge sync and the version guards.
- **CI** (`.github/workflows/ci.yml`): lint (`ruff check`, `ruff format --check`,
  `sync_version.py --check`), the suite on Windows, Linux and macOS across 3.11–3.13
  with `fail-fast: false`, a wheel build that asserts `schema.sql` and the icons made
  it into the archive, and an advisory `mypy` job. A failed run uploads the meret log:
  a hung GUI test says nothing in the pytest output and everything in that file.
- `.github/dependabot.yml`: weekly updates for the hook revisions, the actions and pip.

### Changed

- **`select` is set in `[tool.ruff.lint]` for the first time.** The ignore list named
  `B008`, `RUF012`, `PLW1510`, `BLE001`, `S110`, `TRY004` and `SIM115` — rules from
  sets ruff does not enable by default, so every one of those entries was dead config.
  Turning the sets on brought 447 findings; the deliberate ones are now ignored with
  the reason beside them: function-local imports (259 of them, and the reason `core`
  stays importable without PySide6), `E402` for the same trap, non-ASCII strings, the
  message-only `ValueError`, module-level latches, and the argv-list subprocess rules.
- `tests/**` additionally ignores `S101` and `PLR2004`: an `assert` and a bare number
  are how a test is written, and there is no `-O` run to strip the asserts.
- `[tool.ruff.lint.pylint]` raises `max-args`, `max-positional-args`, `max-branches`
  and `max-statements`: the panel helpers take their widget's fields as arguments, and
  the SQL splitter is long by nature.
- The README badge is no longer edited by hand — `python scripts/sync_version.py`
  writes it, and `--check` is a CI step.
- About fifty findings remain from the newly selected rules (`B904`, `SIM105`,
  `TRY300`, `TRY400`, `B905`, `E501`, `PLW2901`, `E731`, `PLW0108`, `E741`, `SIM108`,
  `TRY301`) and are worked through one rule per commit rather than in one diff.

### Fixed

- **`test_reveal_does_not_list_a_path_it_already_knows` was defined twice** in
  `tests/gui/test_real_panel.py`. The second definition shadowed the first, so the
  first never ran — and its expectation was wrong anyway: `reveal()` always ends in
  `_restore_top()`, so `setRootPath` is called twice and never with the target. The
  shadowed copy is gone.

## \[0.7.31\] — 2026-10-28

### Fixed

- **The `window` fixture in `tests/gui/test_db_switch.py` timed out on the module's
  first test, about one run in six.** The fixture waited for the start-up listing with
  `qtbot.waitUntil`, which re-checks the clock *before* it re-checks the callback.
  Revealing the database folder expands its ancestors, and `%TEMP%` — an ancestor of
  every `tmp_path` — is resolved on the GUI thread with a `stat` per entry, so one such
  event can outlast the whole 2 s budget: the listing landed inside that event and was
  still reported as a timeout. The fixture waits with `pump_until` now, which evaluates
  the predicate once more after its last pump — exactly the turn a landing delivery
  needs — and fails with a sentence instead of a bare `TimeoutError`. It is the same
  helper `tests/gui/test_real_panel.py` already uses for the start-up reveal.

## \[0.7.30\] — 2026-10-28

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

## \[0.7.29\] — 2026-10-27

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
