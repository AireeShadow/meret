# Changelog

Notable changes to meret. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
the version lives in `meret/__init__.py`.

Two rules: one file, one line per patch release unless the change needs explaining.
The repository has no git tags, so older entries are kept as a plain version log
rather than as comparison links.

## \[0.10.0\] - 2026-11-26

### Added

- **AI auto-tagging as a second, independent pass.** `meret aitag` runs a detector over
  the library and tags what it finds; `meret autotag` is untouched and still reads
  nothing but metadata. Two commands, two config sections (`aitag`), two rule files and
  two roots — `metadata/` for what a file says about itself, `ai/` for what a model saw
  in it — so a user without a GPU never pays for the other one's dependencies.
- `meret/core/detectors/`: the abstract half. `Detection` and the `Detector` protocol,
  a registry by name, and the `[detector]` section of a rules file. The YOLO backend is
  one implementation and imports `ultralytics` inside the function that needs it, so
  `core` stays importable without torch.
- Detections are stored in `metadata`, not in a new table: the fact builder already
  flattens one level of nested metadata, so `detect.cat` is an ordinary fact and the
  rule engine needs no knowledge of models at all.
- `detect.<label>` is the best confidence, `detect.<label>.count` is how many, and
  `detect.classes` is every label as one comma-separated string — three keys, because
  "is there a cat", "how many" and "what else" are three different questions.
- `model` and `mtime` in the stored block are the whole cache: a changed file or a
  changed checkpoint re-runs the detector, anything else is skipped. `--force` overrides.
- `ultralytics` is an extra (`pip install 'meret[ai]'`) and its absence is reported
  before the first file rather than during the run.

### Fixed

- `Detector.detect` returns `None` when the model could not answer, and an empty list
  when it looked and found nothing. Both used to be `[]`, which meant a file that
  failed to decode got an empty block cached forever and was never asked about again.

## \[0.9.13\] — 2026-11-16

### Added

- **`docs/core.md`, `docs/autotag.md`, `docs/vfs.md`, `docs/cli.md`.** `gui.md` was the
  only document in `docs/`, and it described a window that no longer exists. The four
  new ones cover the parts a reader has to be told rather than infer: why
  `files.metadata` is a blob nothing searches, why tag uniqueness is per sibling, why
  `iter_files` is a paged generator, why rules are a file and not config fields, why
  the mount is read-only, and why `--lang` is read from `sys.argv` at import time.
- `README.md` links them from a `## Documentation` table, so the set is discoverable
  instead of being five files nobody opens.

### Changed

- `README.md` rewritten for 0.9: an Auto-tagging section (the feature had none),
  `--debug` instead of the diagnostic environment variables, the real-FS panel described
  as one folder plus a drive selector, the progress windows and their Cancel button, and
  a module table for `meret/gui`.
- `docs/gui.md` rewritten: the module map, the mixin typing trick, the progress window's
  three rules, the drive letters from `GetLogicalDrives()`, and a *Traps already paid
  for* section that grew from three entries to six.

## \[0.9.12\] — 2026-11-16

### Added

- **A Cancel button in the progress window, and a cancel that is honest about what it
  can do.** `Indexer.cancel()` sets a `threading.Event` that is checked between files
  while metadata is read and between chunks while rows are written, so a cancel lands
  on a boundary where everything already committed stays committed. `AutoTagger.run()`
  takes `should_stop()` for the same reason, and reports `interrupted` in its stats:
  `Indexer` keeps `self.interrupted`, but a tagger is stateless and can run twice.
- `AbstractRepository.save_files(..., should_stop=None)`: the batch write asks between
  chunks, never inside one. A half-written transaction is the one thing a cancel must
  not leave behind.
- `meret/gui/window/base.py` declares the mixin contract under `TYPE_CHECKING` — the
  widgets `LayoutMixin` builds and the methods the other five provide. It is the only
  place the mixins see each other, and the only place to extend when a member is shared
  by two of them.

### Changed

- **The progress window refuses to close while work runs.** No X, `Esc` ignored, and
  the button asks the work to stop. A window that closes and leaves the work behind it
  is a lie: the poll stays stopped and the application looks dead. It is closed by the
  completion callback, so `dialog.finish()` replaced `dialog.close()` in both handlers.
- **The mixins are typed, not silenced.** Each one inherits `WindowBase` under
  `TYPE_CHECKING` and `object` at runtime, and `MainWindow` lists `WindowBase` last:
  for mypy that is a consistent diamond, for the interpreter a flat chain where
  `super().__init__` walks past the plain classes and lands on the real constructor.
  `mypy meret` went from 237 findings to none, and the ones that survived the first pass
  were real: attributes inferred as `None`, a handler dict joined to `object`.

### Fixed

- **Auto-tagging was slow because every tag was its own transaction.**
  `repo.add_tag()` opens a connection and commits per call — right for a GUI where each
  call arrives on a different thread, catastrophic for a batch. A 10 000-file library
  with three tags each was 30 000 connections and 30 000 commits, each waiting on
  fsync. `add_tags` does one session and one commit per file.
- **The auto-tag progress bar could not move.** `run()` passed `progress(done, None)`,
  so the dialog never left its busy state and the run looked stuck while it worked. The
  total is counted up front: one `COUNT`, or the number of paths when a scan narrows the
  walk, capped by `--limit`.
- **`icons.py` cached `QPixmap` objects, which crashed the suite at random.** A
  `QPixmap` belongs to the `QGuiApplication` that created it; `@lru_cache` kept them
  across applications, and `_build_actions` then drew a pixmap whose application was
  gone — an access violation inside Qt, with no exception and no traceback. The cache
  holds the SVG bytes now, which belong to no application.
- `AutoTagger.run()` had `else untagged_only` where it meant `else only_untagged`, which
  raised `UnboundLocalError` whenever `only_untagged=True` was passed. `mypy` names this
  as `used-before-assignment`; `ruff` cannot, because it is a syntactically valid
  reference to a local name.
- A duplicate `if limit is not None` block in `run()` clamped the total twice, and a
  `noqa: SIM108` sat on the `else` branch while the rule is reported on the `if` — a
  directive in the wrong place reads as a decision and silences nothing.

## \[0.9.11\] — 2026-11-16

### Added

- **A Cancel button in the progress window, and a cancel that is honest about what it
  can do.** `Indexer.cancel()` sets a `threading.Event` that is checked between files
  while metadata is read and between chunks while rows are written, so a cancel lands
  on a boundary where everything already committed is consistent. `AutoTagger.run()`
  takes `should_stop()` for the same reason: the library is left with every processed
  file fully tagged, and the report says "Tagging cancelled" rather than pretending the
  run finished.
- `AbstractRepository.save_files(..., should_stop=None)`: the batch write asks between
  chunks, never inside one. A half-written transaction is the one thing a cancel must
  not leave behind.
- `AutoTagger.run()` returns `interrupted` in its stats. `Indexer` keeps
  `self.interrupted`, but the tagger is stateless — one tagger can run twice — so the
  answer has to travel with the result the GUI already reads.

### Changed

- **The progress window refuses to close while work runs.** The title bar has no X and
  `Esc` is ignored; cancelling is the button, and the button asks the work to stop. A
  window that closes and leaves the work behind it is a lie: the poll stays stopped and
  the application looks dead.
- The window is closed by the completion callback, not by the user, so `dialog.finish()`
  replaced `dialog.close()` in both handlers.
- `test_chunk_size_reaches_the_repository`'s spy and `MockRepository.save_files` accept
  the new keyword. A mock that mirrors a signature has to mirror it completely, or the
  difference shows up as `TypeError` in nine unrelated tests.

### Fixed

- **A duplicate `if limit is not None` block** in `AutoTagger.run()`: the total was
  clamped twice, and the second clamp sat between the two halves of a block that had
  already been replaced by a ternary. Both the duplicate and the `noqa` that tried to
  silence the rule are gone; the ternary is wrapped in parentheses, which is what
  `ruff format` does to it anyway.
- A `noqa: SIM108` that ruff reported as unused: the directive sat on the `else` branch
  while the rule is reported on the `if`. A directive in the wrong place is worse than
  none — it reads as a decision and silences nothing.

## \[0.9.10\] — 2026-11-16

### Added

- **`meret/gui/window/progress.py` — `ProgressDialog`.** A modal window with a label and
  a bar for the two long runs: `scan` shows the indexer's phases (finding files,
  reading metadata, writing to database), `autotag` shows files processed against the
  library's size. The callback the work is given only emits a signal: the indexer and
  the tagger call it on a pool thread, and a widget touched there is a crash, not a
  glitch. Qt queues the signal to the GUI thread, which is exactly the hop needed.
- `AbstractRepository.count_files(untagged_only=False)` and `add_tags(file_id, tags)`.
  Both have working defaults in terms of the existing methods, so a backend that cannot
  do better still works — and both exist to be overridden.

### Changed

- `ProgressDialog` is opened, never `exec()`'d: `exec()` runs a nested event loop, and
  this window has already paid for that once (see `_edit_config`). Modality is a flag,
  `ApplicationModal`, not a loop.
- The dialog reports percentages as soon as the first total arrives, and stays in its
  busy state until then: a bar that claims 0% of something nobody has counted is worse
  than a bar that admits it does not know yet.
- No new locale entries: the titles reuse `"Scanning {path}"` and `"Tagging..."`, and
  the phases reuse the three strings the CLI already prints. Two front-ends naming the
  same phase differently would be two translations of one fact.

### Fixed

- **Auto-tagging was slow because every tag was its own transaction.**
  `repo.add_tag()` opens a connection and commits per call, which is right for a GUI
  where each call arrives on a different thread — and catastrophic for a batch: a
  10 000-file library with three tags each is 30 000 connections and 30 000 commits,
  each waiting on fsync. `add_tags` does one session and one commit per file, which is
  the difference between seconds and minutes. The rule is not broken, it is skipped
  where it does not apply: one operation, one session, however many tags are in it.
- **The auto-tag progress bar could not move.** `run()` passed `progress(done, None)`,
  so the dialog never left its busy state: the run looked stuck while it worked. The
  total is counted up front now — one `COUNT` from the repository, or the number of
  paths when a scan narrows the walk, capped by `--limit`.
- **`icons.py` cached `QPixmap` objects, which crashed the test suite at random.** A
  `QPixmap` is a paint device and belongs to the `QGuiApplication` that created it;
  `@lru_cache` kept them across applications, and `_build_actions` then drew a pixmap
  whose application was gone — an access violation inside Qt, with no exception and no
  traceback. The cache holds the SVG **bytes** now, which belong to no application, and
  a missing application is an explicit `None`/`QIcon()` instead of a doomed render.
- `AutoTagger.run()` had `else untagged_only` where it meant `else only_untagged`, which
  raised `UnboundLocalError` whenever `only_untagged=True` was passed. `mypy` names this
  as `used-before-assignment`; `ruff` cannot, because it is a syntactically valid
  reference to a local name.

## \[0.9.9\] — 2026-11-16

### Added

- **`meret/gui/window/layout.py` — `LayoutMixin`.** `_build_ui`, `_apply_split`,
  `_build_actions`, `_build_toolbar` and `_build_menus` left `app.py`. Nothing there
  decides what a command does — it decides where the command lives, which is why it is
  the one mixin with no business logic in it.

### Changed

- **`meret/gui/app.py` is the assembly now: about ninety lines instead of 1978.**
  It holds the module docstring, `MainWindow` with its `__init__` forwarder, and
  `run_gui`. The window is `class MainWindow(WindowBase, LayoutMixin, ContentsMixin,
  CommandsMixin, DatabaseMixin, MountMixin, PollMixin)`, and the docstring says what
  the order is for: `WindowBase` first, so its `__init__` runs before anything else.
- `BATCH`, `PREVIEW`, `POLL_MS`, `ICON`, `TOOLBAR` and `NAVIGATION_COMMANDS` live in
  the modules that read them — `window/contents.py`, `window/polling.py`,
  `window/constants.py`. `app.py` no longer re-exports any of them.

### Fixed

- Tests stopped importing names from `meret.gui.app` that had moved:
  `RealFsPanel` from `meret.gui.fs_panel`, `TagTree` from `meret.gui.tag_tree`,
  `TOOLBAR`/`ICON` from `meret.gui.window.constants`, `BATCH` from
  `meret.gui.window.contents`. The rule is one line and it has now bitten four times:
  **a monkeypatch, an import and a `from ... import` all have to name the module that
  actually reads or defines the name**, and a module that merely used to hold it will
  fail with `ImportError` or, worse, with a patch that silently does nothing.
- Three handlers were left behind in `app.py` after they had already moved
  (`_show_tree`, `_prune`, `_on_pruned`): the class inherited a working copy from
  `CommandsMixin` and carried a dead one of its own. `Select-String` on the method
  names is what caught it, and it is now part of the checklist for every move.

## \[0.9.8\] — 2026-11-16

### Added

- **`meret/gui/window/commands.py` — `CommandsMixin`.** Every handler left `app.py`:
  `run_command`, `_command_palette`, the tag commands (`_tag_selected`,
  `_untag_selected`, `_autotag`, `_delete_tag`), the scan pair, `_search_query`,
  statistics, sorting, the help dialog and the settings pair (`_edit_config`,
  `_apply_look`).
- **`meret/gui/window/constants.py`** — `ICON`, `TOOLBAR`, `NAVIGATION_COMMANDS`.
  They are read by both the layout and the settings code, and a constant living in
  either of those two modules would make the other import a module it has no business
  knowing.

### Changed

- `MainWindow` is now `class MainWindow(WindowBase, MountMixin, PollMixin,
  DatabaseMixin, ContentsMixin, CommandsMixin)`. What is left in `app.py` is the layout.
- Every handler reports through `_say()` instead of `_status_label.setText()`:
  "Select a real file first!", "Scanning {path}...", "Tagging...", "Sort by: {mode}",
  "Deleted tag {path}" and the rest. The rule is now uniform — a message written with
  `setText()` alone survives about one poll, because `_update_status()` runs after
  almost every action.
- `_build_actions` keeps connecting `self.run_command`, which now resolves through
  `CommandsMixin`. A missing mixin in the bases fails on the first click, not on
  import, so the class declaration is load-bearing.

### Fixed

- `tests/gui/test_settings.py` patches `apply_theme` in
  `meret.gui.window.commands` now: `_apply_look` moved there and is its only caller.
  With the patch left on `meret.gui.app`, the real theme was applied and the counter
  stayed empty — a test that passes for the wrong reason until it is read.

## \[0.9.7\] — 2026-11-16

### Added

- **`meret/gui/window/contents.py` — `ContentsMixin`.** The browser left `app.py`:
  `_refresh`, `open_folder`, `_show_nodes`, `_add_file_item`, `_pump`, the icon and
  tooltip helpers, `_update_status`, `_say`, `_show_error`, the search pair
  (`_run_search`, `_show_files`), the preview pair (`_show_preview`, `_set_preview`),
  the opening helpers and the viewer stepping. `BATCH`, `PREVIEW` and `_reason_text`
  went with them — the two constants and the tooltip table were only ever read here.

### Changed

- `MainWindow` is now
  `class MainWindow(WindowBase, MountMixin, PollMixin, DatabaseMixin, ContentsMixin)`.
  What is left in `app.py` is the layout and the commands — the last two mixins.
- `_step_file` and `_on_real_activated` report through `_say()` instead of
  `_status_label.setText()`. "End of the folder" and "File is not in the database"
  are read exactly when nothing else is happening, and they used to be wiped by the
  next status refresh — the same bug `_say()` was written for, in two more places.

### Fixed

- `_reason_text` was left behind in `app.py` after the move, and `Reason` had already
  been dropped from that module's imports: the function raised `NameError` on the
  first file without a preview. A move is not done until the old copy is deleted, and
  `ruff` names the leftover as `F821 Undefined name` — which is why the lint step goes
  before the tests, not after them.

## \[0.9.6\] — 2026-11-15

### Added

- **`meret/gui/window/databases.py` — `DatabaseMixin`.** `_init_db`, `_use_db`,
  `_prepare_db`, `_switch_to`, `_on_db_failed`, `_open_db`, `_wipe`, `_drop_objects`,
  `_create_elsewhere` and `_confirm_overwrite` moved out of `app.py`. The split follows
  what the methods already did: opening and wiping run on the task pool, and only the
  swap touches Qt, after the pool has answered.

### Changed

- `MainWindow` is now `class MainWindow(WindowBase, MountMixin, PollMixin, DatabaseMixin)`.
  `app.py` keeps the browser and the commands — the next two mixins to move.
- `_init_db` and `_create_elsewhere` report through `_say()` instead of
  `_status_label.setText()`. "Database recreated!" and "Database: {path}" used to be
  wiped by the poll's next refresh, so the answer to "did it work?" disappeared after
  about a second and a half — the same bug `_say()` was written for.
- `gc`, `sqlite3` and `QFileDialog` are no longer imported by `app.py`: they went with
  the methods that used them.

### Fixed

- `tests/gui/test_db_switch.py` patches `SQLiteRepository` in
  `meret.gui.window.databases` now. A monkeypatch has to target the module that looks
  the name up: with the patch left on `meret.gui.app`, the real repository opened on
  the first try, the switch succeeded, and the attempt counter stayed at zero — which
  reads as "the retry never happened" instead of "the patch did nothing".

### Removed

- The duplicated `TOOLBAR` docstring in `app.py`: the same comment block appeared twice
  in a row, which is how the duplicated-method bugs of 0.9.5 started.

## \[0.9.5\] — 2026-11-15

### Added

- **`meret/gui/window/` — the window, split by responsibility.** `WindowBase` holds the
  state, the first paint and the shutdown; `PollMixin` the poll that notices writes
  from other processes; `MountMixin` the shell-outs. Each one is a plain class with no
  state of its own: everything it touches is created in `WindowBase.__init__`, so the
  split needs no constructor arguments and no wiring.
- `meret/gui/theme.py`: `apply_theme()` and the two palettes, out of `app.py`. The
  theme has nothing to do with the window, and keeping it there meant `app.py` imported
  `QCoreApplication` only for a type hint.

### Changed

- `MainWindow` is now assembled: `class MainWindow(WindowBase, MountMixin, PollMixin)`.
  Its `__init__` does one thing — `super().__init__(repo, db_path, parent)`.
- `_apply_theme` became the public `theme.apply_theme`; `_apply_look` calls it by that
  name.
- The CLI plumbing lives in `mounts.py` with `CLI_TIMEOUT` next to it: `_cli`,
  `_cli_call`, `_run_async`, `_spawn`. `_run`, `_run_cli` and `_spawn_cli` are gone.

### Fixed

- **The menu bar showed every menu twice.** `MainWindow.__init__` kept the old body —
  `_build_ui`, `_build_actions`, `_build_toolbar`, `_build_menus` — *and* called
  `super().__init__`, so `WindowBase` built everything a second time. The menu bar is
  one object per window, so `addMenu` ran twice on it and every menu appeared twice;
  `self.toolbar` and `self.grid` were merely overwritten by the second copy, which is
  why only the menus gave the duplication away (36 entries against a table of 18).
- **Settings ran every callback twice.** `_edit_config` contained its whole body twice,
  so `finished` was connected twice: a save applied the values, restarted the poll and
  ran the callback again. One body, one connection.
- **`_umount` still blocked the GUI thread.** It called `_run`, the synchronous
  `subprocess.run`, and `meret umount` finds its target with PowerShell on Windows —
  seconds of WMI enumeration. It goes through `_run_async` now, so the window keeps
  answering while the CLI works.
- **`TagTree` corrupted the heap (0xc0000374).** A listing delivered from the task pool
  can land after the tree — or the whole window — was destroyed, and `clear()` deletes
  every `QTreeWidgetItem`. The callback still held one, and `takeChildren()` on a
  deleted item is a crash inside Qt, not an exception: `QTreeWidgetItem` is not a
  `QObject`, so PySide cannot raise `RuntimeError` for it. `_filled` and `_failed` ask
  `shiboken6.isValid()` for both the tree and the item first — the same guard
  `RealFsPanel` already uses for its probe results.

### Removed

- `MainWindow._open_selected`, dead since the viewer took over opening files, and the
  duplicated `TOOLBAR` docstring.

## \[0.9.4\] — 2026-11-15

### Added

- **`autotag` in both front-ends.** A toolbar button, a menu entry, the palette and
  `Ctrl+Shift+A` in the window; `a` and a palette row in the TUI. The TUI shells out
  to `meret autotag` instead of importing `AutoTagger`, the same way `scan` and
  `mount` do: the rules and the config already live in the CLI, and a second path to
  the same policy is a second place for it to drift. The window runs it on the task
  pool with the poll stopped, and reports the run's own counters.
- **Default auto-tagging rules ship with meret** (`meret/core/rules/default.toml`,
  nine rules): kind, format, orientation, two resolution buckets, year, camera make,
  long video, screenshot. Used only when neither `autotag.rules_file` nor `--rules`
  names a file, so a fresh install does something instead of printing "no rules". A
  configured file that is missing stays missing, with a warning — falling back to a
  default policy would tag a library nobody asked it to tag.
- `"autotag": ("tags", "sparkles", "wand-sparkles")` in `scripts/fetch_icons.py`, so
  the glyph is vendored as `autotag.svg` and `prune()` keeps it. Without a file the
  button still works: `command_icon()` falls back to a tile with the initial.
- `tests/test_autotag.py::test_the_shipped_rules_parse_and_tag_a_photo`, with staged
  assertions: "no tags" says nothing about whether the file failed to parse, the rules
  failed to match, or the template rendered empty.

### Fixed

- **Messages on the status line survived about one and a half seconds.** `_on_db_failed`
  wrote "Error: database is locked" with `setText()`, and the poll's next refresh
  called `_update_status()`, which rewrote the line to "No selection" — the message
  was technically shown and practically never read. `_say()` now stores a notice that
  `_update_status()` prefers, and `run_command()` clears it, so a message lives until
  the user asks for something else. The same bug hit `Scan complete!`,
  `Tags deleted: N`, the statistics line and the `umount` result; all four go through
  `_say()` now.

### Changed

- `meret/cli/tui.py` gains `action_autotag`, the binding, the palette row and a help
  line. `tests/gui/test_commands.py` is strict again: the GUI and the TUI command
  tables match one to one, and `untag` stays the only exception (the TUI lists it but
  never wires it up).

## \[0.9.3\] — 2026-11-15

### Added

- **`--debug <name>[,<name>]` replaces the diagnostic environment variables.**
  `meret --debug watchdog,watch_fs gui` does what `MERET_WATCHDOG` and
  `MERET_WATCH_FS` did, except a misspelled name is a command-line error listing the
  real ones instead of a silent no-op — which is what a misspelled environment
  variable always was. Names: `watchdog`, `watch_fs`, `settings_modeless`,
  `media_log`, `no_tooltips`. The environment is still read as a fallback, so an
  existing script keeps working; the flag wins when both are present.
  `meret/core/debug.py` holds the registry, `parse()` and `enabled()`.
- `meret/gui/drives.py`: the drive letters, read from `GetLogicalDrives()`.

### Changed

- **`meret/gui/app.py` is split.** `RealFsPanel` → `gui/fs_panel.py`, `TagTree` →
  `gui/tag_tree.py`, the traced application and the media-log switch →
  `gui/diagnostics.py`, the drive letters → `gui/drives.py`. `app.py` keeps the window
  and the entry point. Nothing moved changes behaviour, but the split is what made the
  bug below findable: the trace finally named a file instead of a 1978-line module.
- The diagnostic switches are read through `debug.enabled()` instead of
  `os.environ.get()` at each call site, so a switch has one spelling and one place to
  look when it does not seem to work.

### Fixed

- **Enumerating or probing drives could take the process down, not just slow it.**
  `QDir.drives()` builds a `QFileInfo` per drive, and on a disconnected mapped drive
  the volume query faults inside the network provider: an access violation below the
  level where Python can raise, so no `except` clause and no lock helps. Probing the
  drives first (a thread each, `os.path.exists()`, later `GetDriveTypeW()`) crashed
  the suite three times — the last two from the same call, and one of them on the GUI
  thread, inside `QDir.drives()` itself. The letters now come from
  `GetLogicalDrives()`, a bitmask read from the mount manager: no drive is opened, no
  volume is queried, and a dead drive costs nothing until someone selects it.
- `ctypes.windll.kernel32.GetDriveTypeW` is bound once at import with explicit
  `argtypes`/`restype`, and the panel no longer reaches ctypes from more than one
  thread. Two probes entering `windll`'s lazy loader at the same time was an access
  violation of its own — the trace pointed straight at that line.
- `RealFsPanel` no longer crashes when `setRootPath()` answers with `None`: a test
  double that returns nothing used to reach `setRootIndex()` and raise `TypeError`.

### Removed

- The drive probe: `_probe_drives`, `_probe_drive`, `_reachable`, `_probing`,
  `drive_probed`, and the four tests that described them. It was the only way to grey
  out a dead drive in the selector, and it was also the only native call in the GUI
  that could kill the process. The `Not reachable` locale string went with it.
- `tests/gui/test_real_panel.py` tests that patched `app.threading` or `app.QDir` now
  patch `fs_panel` — a monkeypatch has to target the module that *looks the name up*,
  not the one that used to define it.

## \[0.9.2\] — 2026-11-14

### Fixed

- **The GUI froze for ~5.5 s and sometimes never came back, on a machine with two
  unreachable mapped drives.** `QFileSystemModel` builds its own root node as the
  drive list, and building it asks Windows for every drive's label and free space.
  A dead mapped drive answers after ~5.5 s (`X:` and `Y:` in the trace), and the
  whole delivery arrives on the GUI thread in a single event. A blocked GUI thread
  does not repaint, so the tooltip painted a moment before stayed frozen on screen —
  the freeze looked like hovering the toolbar, which is where the report started.
- **The panel is rooted at exactly one folder now, never at the model's own root.**
  `setRootPath()` makes a path the root node, so the drive list is never built.
  `index(path)` looks equivalent and is not: it resolves the path through the root
  node, which is the drive list — that was the difference between a freeze and no
  freeze, and it is why the first attempt at this fix changed nothing.
- **The start-up reveal is gone.** `reveal(self._db_path.parent)` expands every
  ancestor of the database folder, and each expand is a listing with a stat per
  entry on the GUI thread; with the database in a synced folder those stats go
  through the sync filter. The folder is logged instead, and the panel starts at the
  home folder.
- **`umount` no longer runs the CLI on the GUI thread.** `_pids_for_mountpoint` finds
  its target with `Get-CimInstance Win32_Process` on Windows — seconds of WMI
  enumeration, and no natural end when the WMI service is unhappy. The call runs on
  the task pool with `stdin=DEVNULL` and a timeout, and the process scan gives up
  after ten seconds instead of waiting forever.
- The status line no longer claims "Unmounted" before the CLI has answered:
  `meret umount` exits 1 with "Nothing is mounted at ...", and that is what the
  window shows now.
- `reveal()` no longer reports success for a path that is not on disk. The check is
  on the path, not on `fileInfo(index)`: `setRootPath()` on a missing path hands
  back the node of the nearest existing ancestor, so the old check answered about
  the parent and said True.
- `RealFsPanel` no longer crashes when `setRootPath()` answers with `None` — a test
  double that returns nothing used to reach `setRootIndex()` and raise `TypeError`.

### Added

- **A drive selector above the real-FS panel.** The drives come from
  `QDir.drives()` — names only, instant — and each one is tested by the probe that
  already ran for the panel. A drive that does not answer is disabled in the list,
  so it cannot be selected and the model is never pointed at it. This is the only
  way to offer the drives without paying for the model's own drive list.
- `RealFsPanel.drive_probed`, so the window can act on a probe result instead of
  only the panel.
- `[watchdog] stuck for N s in: <receiver>/<event>` with `MERET_WATCHDOG=1`. A
  handler that never returns never reaches `notify()`'s `finally`, so no `slow:`
  line is printed exactly when a freeze is worst — this line names the event that
  is still running, read by the watchdog thread while the GUI thread cannot answer.
- `MERET_NO_TOOLTIPS=1`, which ruled the tooltip path out: the freeze was identical
  without any tooltip.

### Changed

- `refresh()` re-reads the current root by stepping through its parent, since
  `setRootPath()` with the path it already has returns early. It refuses to start a
  second listing while one is running: a nested event loop that delivers an event
  mid-listing used to stack a second listing on top of the first, and the queue
  never drained again.
- `_spawn` passes `stdin=DEVNULL`. `meret mount` waits for Enter on Windows, and a
  window has no console to inherit one from — leaving stdin attached is how a mount
  ends the moment it starts.
- `_run`, `_run_cli` and `_spawn_cli` are gone: the first was the only blocking
  subprocess call left on the GUI thread, and the other two were never called.
- `tests/gui/test_real_panel.py` describes the new behaviour, and two of its tests
  are regression guards rather than descriptions: `test_the_panel_never_asks_for_the_drive_list`
  fails if anything ever passes `""` to `setRootPath()` again, which is the exact
  call that cost 5.5 s per dead drive.

## \[0.9.1\] — 2026-11-10

### Added

- **`autotag.root`: every produced tag is filed under a configurable parent.**
  `metadata` by default, so auto-tags never mix with hand-made ones at the top of
  the tree. `--root` overrides it for one run; an empty root files them bare.
- A rule that already spells the root out — `metadata/camera/{exif.Make}` — is not
  prefixed twice, so both spellings of the same rule agree.
- The root is normalised on load: `/metadata/`, `metadata` and ` metadata ` are one
  spelling, and backslashes count as separators. The value is typed by a human into
  a config file, and a leading slash is not a hierarchy.
- Dedupe in `plan()` compares the final path, root included — otherwise `--root`
  would only change what gets written while the "file already has this tag" check
  looked at a different name, and a second run would add a duplicate.
- A "Tag root" row in the settings dialog, and coverage in `tests/test_autotag.py`
  for prefixing, double-prefixing, an empty root and normalisation.

### Fixed

- **`AutoTagConfig` was defined twice in `meret/core/config.py`.** The second
  definition rebound the module name, but `MeretConfig.autotag` had already
  captured the *first* class object when the field was declared — so `root` never
  reached the model. The dialog showed the row, `values()` returned it,
  `apply_settings()` wrote it, and `MeretConfig` dropped it on validation. Nothing
  raised: a duplicate class definition is silent in Python.
  `test_every_config_field_has_a_widget` caught it by comparing the model's fields
  against the dialog's table instead of trusting either side.

## \[0.9.0\] — 2026-11-10

### Added

- **Metadata auto-tagging.** A rule is a tag path plus conditions over a file's
  extracted metadata, so `exif.Make = Canon` can produce `camera/Canon` without
  anyone clicking through the library. `meret/core/autotag.py` holds the rules,
  the matcher and the tagger; `core` still imports neither `vfs` nor `cli`, so the
  layering rule survives the feature.
- **Rules are data, and they live in a file.** `[[rules]]` in a TOML/JSON file,
  pointed at by `autotag.rules_file` or `--rules`. Not in `config.toml` on
  purpose: rules are a list of lists, and the settings dialog shows exactly one
  widget per config field — a rules table is not something a form row can hold.
  `test_every_config_field_has_a_widget` is what settled that argument.
- **Conditions see a flat view of the metadata** — dotted keys (`exif.Make`,
  `tags.artist`, `creation_time`) plus derived facts (`type`, `orientation`,
  `aspect`, `megapixels`, `duration_s`, `duration_min`, `year`), so nobody has to
  write a rule against `2021:07:14 18:02:11` to tag a year.
- **Thirteen operators:** `eq ne in not_in contains startswith endswith regex
  gt gte lt lte exists missing`. They are a dispatch table, not a chain of
  `if`s — one row per operator, and `matches_condition` only decides whether the
  fact is there at all.
- **Tag templates.** `camera/{exif.Make}` is skipped when the placeholder has no
  value, so a rule can never create a bare `camera/` tag for `tag-prune` to
  clean up later. EXIF strings are stripped on the way in: `"Canon "` would
  otherwise become a tag with a trailing space.
- `meret autotag [--rules FILE] [--dry-run] [--only-untagged/--all] [--limit N]`
  tags an already-indexed library.
- `meret scan --autotag` runs the same rules over the files the scan just
  touched, and `autotag.enabled` is the default for the flag — `--autotag/
  --no-autotag` is an `Optional[bool]` because "not given" and "explicitly off"
  are different answers when the config has an opinion.
- **A group in the settings dialog:** enabled, only-untagged and the rules file.
  Its browse button opens a file rather than offering to save one — a save dialog
  would propose overwriting the rules it is being asked to point at.
- `tests/test_autotag.py`: facts and derived facts, every operator, a bad regex,
  template rendering, dedupe against existing tags, rule colours, the dry run,
  the path filter, `only_untagged`, and rule loading from both TOML and JSON.
- Locale entries for the new strings in both catalogs.

### Changed

- **`AbstractRepository.iter_files` is not abstract.** A paged generator, so a
  whole-library pass never materialises the library in memory — and deliberately
  overridable-by-omission: a storage that cannot enumerate its files (a test
  double, a backend that only answers lookups) must not become uninstantiable
  because of a feature it never uses. It raises `NotImplementedError` at the
  moment someone walks it. Making it abstract broke every mock repository in the
  suite on the first attempt, which is exactly the cost of a contract nobody
  asked that double to honour.
- `Indexer` takes an optional `AutoTagger`; without one it behaves exactly as
  before, so the GUI and the TUI need no changes at all.
- **A dry run reports the same keys it always did.** `scan --dry-run` still
  returns `indexed updated errors found skipped interrupted` and nothing else:
  the CLI reads those in both branches, and an extra key is how the two branches
  start to drift. What auto-tagging *would* do is `meret autotag --dry-run`'s
  answer, with its own counters.

### Fixed

- EXIF and audio tag values are stripped before they become facts, so a padded
  `Make` cannot produce a tag with trailing whitespace.
- A missing rules file is a logged warning and "no rules", not a traceback: the
  config can point at a file on a drive that is not mounted right now.

## \[0.8.5\] — 2026-11-09

### Added

- **The arrow keys step, and Space plays.** Right is ten seconds forward inside a
  video and the next file outside one; Left is ten seconds back and the previous file.
  Space and Enter toggle playback.
- The keys are `QAction` shortcuts with `WidgetWithChildrenShortcut`, not a
  `keyPressEvent` override. A focused `QSlider` eats the arrows to nudge its handle by
  one step, and a focused button eats Space and Enter to click itself — a shortcut is
  matched before the key reaches the focus widget, so one meaning wins everywhere in
  the viewer and none of them leak into the browser tab.
- The page itself takes `StrongFocus`, and opening a file gives it the focus: without
  that the shortcuts have nothing to scope to.

### Changed

- `JUMP_MS` (10 s) is both the arrow step and the scrub bar's page step, so Page
  Up/Down and the arrows move by the same amount.

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
