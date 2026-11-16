# The Qt window

Why `meret/gui` looks the way it does. Almost every shape here was paid for with a
freeze, a lost drive letter or a crashed test run, so read this before changing any of
it.

## Boundaries

- **Qt lives only in `meret/gui`.** `core`, `vfs` and `cli` stay importable without
  PySide6; `tests/gui/test_architecture.py` guards it.
- **The GUI never imports a mount backend.** Mounting shells out to `meret mount` /
  `meret umount`, so opening a window does not drag FUSE or WinFsp in.
- **Nothing touches SQLite or decodes an image on the GUI thread.** Both go through
  `TaskRunner`.
- **No native call in the GUI enumerates drives.** `QDir.drives()`, `QFileInfo` on a
  letter and `os.path.exists()` on a mapped drive can all fault inside the network
  provider — an access violation below the level where Python can raise, so no `except`
  clause and no lock helps. The letters come from `GetLogicalDrives()`; see *The real
  filesystem panel*.

## Module map

`meret/gui/app.py` is the assembly, not the window: it holds `MainWindow` and
`run_gui`, and nothing else. `MainWindow` is built from mixins, one per reason to
change.

| File | Responsible for |
|------|-----------------|
| `window/base.py` | the state, the first paint, the shutdown, and the mixin contract |
| `window/layout.py` | panels, grid, preview, toolbar, menus |
| `window/contents.py` | listings, thumbnail batches, preview, search, the status line |
| `window/commands.py` | the command handlers, the settings dialog, the help |
| `window/databases.py` | opening, wiping and switching the database |
| `window/mounts.py` | the shell-outs to `meret mount` / `meret umount` |
| `window/polling.py` | the poll that notices writes from other processes |
| `window/constants.py` | the toolbar order and the icon size |
| `window/progress.py` | the modal progress window and its Cancel button |
| `fs_panel.py`, `tag_tree.py` | the two panels |
| `diagnostics.py` | `TracedApplication` and the media-log switch |
| `drives.py` | the drive letters, from `GetLogicalDrives()` |
| `theme.py` | the palette |
| `thumbnails.py`, `viewer.py`, `workers.py`, `icons.py` | decoding, playback, the pool, the glyphs |

A mixin is a plain class with no state of its own: everything it reads is created by
`WindowBase.__init__`, which is why the split needs no constructor arguments and no
wiring. The attributes *are* the contract.

## Threads: TaskRunner

`meret/gui/workers.py` runs a callable in a `QThreadPool` and delivers the result on
the GUI thread through a `QueuedConnection`.

- Every task carries a **generation**. A panel that re-asks (another tag, another
  folder) calls `bump()`, and results from an older generation are dropped instead of
  painting over fresher rows.
- **`sticky=True`** exempts a task from that: a version probe, a scan, an auto-tag run
  or a database switch must still report even though the user kept browsing while it
  ran.
- **Lifetime rules, learned the hard way.** `QRunnable` is not a `QObject` and the
  pool does not own it: module-level `_INFLIGHT` keeps every task alive from `start()`
  until its result is delivered, a task keeps its runner alive, and `clear()` releases
  dropped runnables itself — otherwise the safety net would hold them and their
  closures for the whole run.
- **A callback that touches Qt must run on the GUI thread.** Progress reporting is the
  exception that proves it: the work calls its callback on a pool thread, so the
  callback only emits a signal and the receiver does the painting. See *The progress
  window*.

## The tag tree

`TagTree` reads children only when a row is expanded, and off the GUI thread:
`listdir` can rebuild the whole repository snapshot, and doing that on a library with
thousands of tags freezes the window mid-click.

- A row that is being read shows `…`; a row that failed shows the error text. A row
  that looks empty is a bug.
- After a listing lands — or fails — the row is marked `FILLED`. That is what stops
  the next expand from queueing the same broken listing again: **F5 (`reload`) is the
  retry.**
- The tree keeps its own generation, so a listing still in flight when the tree is
  rebuilt never touches a row that is already gone.
- **`shiboken6.isValid()` guards both the tree and the row** before anything is
  touched. `clear()` deletes every `QTreeWidgetItem`, and a delivery can land after
  that — or after the whole window is gone. `QTreeWidgetItem` is not a `QObject`, so
  PySide cannot raise `RuntimeError` for it; calling `takeChildren()` on a deleted item
  corrupts the heap (`0xc0000374`) instead. That crash is why the guard exists.
- `set_tree(None)` is not a detail: the old tree holds the old repository, and a
  database file cannot be deleted while a connection to it is open.

## The real filesystem panel

`RealFsPanel` is a plain `QTreeView` over Qt's own `QFileSystemModel` — no database,
no pool thread. Four decisions in it are load-bearing:

- **The watcher is off** (`DontWatchForChanges`). `QFileSystemModel` watches every
  directory it has ever listed, and a change anywhere in one of them makes it re-read
  that directory on the GUI thread with a `stat` per entry — measured at **6.4 seconds
  inside a single `QEvent`**. `DontResolveSymlinks` and `DontUseCustomDirectoryIcons`
  (an `SHGetFileInfo` call per entry on Windows) go with it. `--debug watch_fs`
  restores the stock model for an A/B run.
- **The panel is rooted at exactly one folder, never at the model's own root.** The
  model's root is the drive list, and building it asks Windows for every drive's label
  and free space: a disconnected mapped drive answers after ~5.5 s, and the whole
  delivery lands on the GUI thread in a single event. `setRootPath(path)` makes the
  path the root node, so the list is never built. **`index(path)` looks equivalent and
  is not** — it resolves the path *through* the root node, which is the drive list.
  That one-word difference was the difference between a freeze and no freeze.
- **The drives come from `GetLogicalDrives()`**, a bitmask read from the mount
  manager, and the window's selector offers them from names only. Nothing is probed:
  the panel used to run a thread per drive asking `os.path.exists()`, later
  `GetDriveTypeW()`, so a dead drive could be greyed out — and both calls can fault
  natively on a disconnected mapped drive. The feature was dropped rather than kept at
  the price of a process that dies at random. A dead drive now costs one slow listing
  when someone selects it: a deliberate click, never a probe on a timer.
- **Navigation *reveals*, it does not re-root.** `reveal()` tries `index()` first (a
  known path is not listed again) and falls back to `setRootPath()`, then expands the
  ancestors top down and scrolls the row into view. Ancestors that are already open are
  skipped: expanding an open row repeats the whole `stat` cost, and `%TEMP%` alone is
  tens of thousands of entries. A path that is not on disk is refused — `setRootPath()`
  hands back the node of the nearest existing ancestor, so a check on
  `fileInfo(index)` answers about the parent and says True.
- **`refresh()` refuses to start a second listing while one is running.** A nested
  event loop can deliver an event mid-listing, and a second listing on top of the first
  is how the queue stops draining.

## Switching the database

`_use_db()` puts the work on the pool and returns: opening and wiping both retry with
a `time.sleep` while a task in flight still holds the file — up to half a second of a
frozen window. `_switch_to()` runs in the callback and does the Qt part (repository,
tree, thumbnail scope, re-read); `_on_db_failed()` writes a status line and leaves the
old database in place. The task is sticky, so a switch is not eaten by browsing.

## The progress window

`window/progress.py`, used by `scan` and `autotag`. Three rules:

- **The callback only emits.** The indexer and the tagger report from a pool thread, so
  `ProgressDialog.report()` does nothing but `advanced.emit(phase, done, total)`; the
  receiver updates the widgets on the GUI thread. A widget touched in the callback is a
  crash, not a glitch.
- **The report is throttled** to ten steps a second, and the last step is always sent.
  One report per file means ten thousand queued events and ten thousand repaints: the
  window stops answering long before the run ends, which reads as "closing the dialog
  froze the application". Dropping the final event instead leaves the bar at 97%, which
  reads as a hang — so the throttle has that exception.
- **The window cannot be closed while work runs.** No X, `Esc` ignored, and a Cancel
  button that asks the work to stop. Neither the indexer nor the tagger can be killed
  from here, so a window that closes and leaves the work behind it is a lie: the poll
  stays stopped and the application looks dead. It is closed by the completion
  callback, through `finish()`.
- **Cancel is a flag in the core, not a killed thread.** `Indexer.cancel()` sets a
  `threading.Event` checked between files and between chunks; `AutoTagger.run()` takes
  `should_stop()`. A cancel therefore lands where everything already committed is
  consistent — a scan is not a transaction, and what was written stays written.

## The settings dialog

`_edit_config()` calls `open()`, never `exec()`: `exec()` runs a nested event loop and
the poll, the pool and the dialog end up fighting over the same window. The
`raise_()` / `activateWindow()` pair is **deferred by one turn** — on Windows the owner
can take the foreground back in the same turn, and the dialog ends up behind a window
that modality has already blocked, which reads as a dead app.
`--debug settings_modeless` removes native modality for an A/B run.

While the dialog is up, `_paused` stops the thumbnail pump: the grid underneath is not
looked at, and every icon that lands repaints it. `finished()` resumes the pump. That
handler is connected **once**: a duplicated body used to connect it twice, so every
save applied the values, restarted the poll and ran the callback again.

## The status line

`_say(text)` stores the message in `_notice` and sets the label; `_update_status()`
prefers `_notice` over the selection count, and `run_command()` clears it. Without
that, a message written with `setText()` survived about one poll — "Error: database is
locked" was replaced by "No selection" a second and a half later, so it was technically
shown and practically never read.

## Thumbnails

Decoding happens in the pool, in batches of `BATCH`. The next batch is asked for by
the **last landing result**, not by a zero-timer — a zero-timer refills the queue
immediately, so the whole folder ends up in the pool and `BATCH` limits nothing. The
preview takes its own token per request, so a slow decode cannot overwrite a newer
selection.

## Icons

`icons.py` caches the SVG **bytes**, never a `QPixmap`. A pixmap is a paint device and
belongs to the `QGuiApplication` that created it; `@lru_cache` on the rendered pixmap
kept them across applications, and the test suite builds and drops applications between
modules. `_build_actions` then drew a pixmap whose application was gone — an access
violation inside Qt, with no exception and no traceback, and it only happened sometimes.
With no application at all, the render returns `None` and the caller falls back to a
drawn tile.

## Diagnostics

`--debug` turns them on, before the command name: `meret --debug watchdog gui`. A
misspelled name is a command-line error listing the real ones, which is the whole point
— `MERET_WATCHDOGG=1` used to be a silent no-op. The environment variables still work
as a fallback.

| Name | Effect |
|------|--------|
| `watchdog` | dumps every thread's Python stack every 3 s and runs `TracedApplication`: an event rate, the slowest event, its widget chain, the focus owner, and `stuck for N s in: <widget>/<event>` |
| `watch_fs` | keeps the filesystem watcher (stock `QFileSystemModel`) |
| `settings_modeless` | settings as a modeless window |
| `media_log` | keeps the FFmpeg backend's log, which is off by default |
| `no_tooltips` | drops the toolbar tooltips, for an A/B run |

The watchdog report is printed from a **daemon thread**, so it keeps coming while the
GUI thread is the thing that is stuck. Read it as: `0 events/s` means the thread is
blocked outside event handling; thousands of events per second means a layout or paint
storm. The two need opposite fixes, and the rate tells them apart.

`stuck for N s in:` exists because a handler that never returns never reaches
`notify()`'s `finally`, so no `slow:` line is printed exactly when a freeze is worst.
The daemon thread reads the event that is still running and names it — that line is
what found the last of the freeze bugs, after two were found by elimination.

`media_log` is the only one that is not a stall diagnostic: the FFmpeg backend logs its
version, its codec table and every misdetection to stderr, and a window has no console
to show any of it. `quiet_media_logs()` sets `QT_LOGGING_RULES` before `QApplication`
exists — the rules are read at construction — and leaves `warning` and `critical` on,
so a file that will not open still says so.

## Typing the mixins

The mixins are plain classes at runtime and `WindowBase` subclasses under
`TYPE_CHECKING`:

```python
if TYPE_CHECKING:
    from meret.gui.window.base import WindowBase
else:
    WindowBase = object  # shadows the class on purpose
```

and `MainWindow` lists `WindowBase` **last**:

```python
class MainWindow(
    LayoutMixin, ContentsMixin, CommandsMixin, DatabaseMixin, MountMixin, PollMixin, WindowBase
):
```

Both halves matter.

- For mypy: every mixin inherits `WindowBase`, so listing it first would put a parent
  before its own subclasses and the MRO cannot be linearised. All subclasses, then the
  parent, is the only order that resolves.
- At runtime: the mixins are `object` subclasses, so the chain walks past all of them
  and lands on `WindowBase.__init__` — the only real constructor here. Reversed,
  `super().__init__(repo, db_path, parent)` would reach `object.__init__` with three
  arguments.

`WindowBase` also declares the contract in a `TYPE_CHECKING` block: the widgets
`LayoutMixin` builds and the methods the other five provide. It is the only place the
mixins see each other, and the only place to extend when a member is shared by two of
them. `mypy meret` went from 237 findings to none with this, and the ones that survived
the first pass were real: attributes inferred as `None`, a handler dict joined to
`object`, `_Base` used as a base class.

## Testing rules

- The suite runs headless (`QT_QPA_PLATFORM=offscreen`).
- Every modal dialog is stubbed: a dialog runs its own event loop, and nothing clicks
  in a headless run.
- `pump()` is a nested `QEventLoop` with a timer. `processEvents()` without a limit
  does not return while the event stream is alive, which is how the suite used to
  hang. `pump_until()` evaluates its predicate once more after the last pump — the turn
  a late delivery needs.
- Background threads must print to the file descriptor (`os.write(2, ...)`), never to
  `sys.stderr`: pytest replaces and closes it, and the thread then raises
  `OSError: [WinError 6]`.
- A test that waits for async work waits for the **observable effect** (file created,
  tags gone), not for a path that never changed.
- **A monkeypatch targets the module that looks the name up**, not the one that used to
  define it. `SQLiteRepository` is patched in `window.databases`, `subprocess` in
  `window.mounts`, `apply_theme` in `window.commands`. A patch on `meret.gui.app`
  creates an attribute nothing reads and leaves the real object in place — the test
  then passes for the wrong reason, or fails on a counter that stayed at zero.
- Anything that hands a `QTreeWidgetItem` or a `QListWidgetItem` to a pool callback
  needs a `shiboken6.isValid()` guard: those are not `QObject`s, so a delivery after
  destruction corrupts the heap instead of raising.

## Traps already paid for

- **`in` on a `sqlite3.Row` tests values, not column names** (cpython#100450). `row`
  is not a dict and has no `get()`. `_tags_from_row` holds `row.keys()` in a local for
  exactly this reason — otherwise ruff's SIM118 "simplifies" it back and every file
  comes back untagged.
- `QFileSystemModel` populates on its own thread: `rowCount()` is 0 until the listing
  lands, so tests wait instead of asserting straight away.
- A `QTimer.singleShot` outlives the widget it was started from: callbacks touch
  `self` inside `try/except RuntimeError`.
- **`ctypes.windll.kernel32.X` is resolved lazily on first attribute access.** Two
  threads reaching that loader at the same time is an access violation. Bind the
  function once at import, with explicit `argtypes`/`restype`.
- **`setRootPath()` on a missing path hands back the nearest existing ancestor**, so a
  validity check on the returned index says "fine" for a path that is not there.
- **A duplicated method body is invisible in review and doubles the effect.** A second
  copy of `_edit_config` connected `finished` twice; a second copy of `_build_menus`
  put eight menus on one bar. `ruff` names the first as `F811`; the second only shows
  up in a test that counts.
